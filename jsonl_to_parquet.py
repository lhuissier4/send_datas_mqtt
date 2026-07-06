import glob
import hashlib
import os
import time
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count

import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from tqdm import tqdm

load_dotenv()

BRONZE_DIR = os.environ["BRONZE_DIR"]
SILVER_DIR = os.environ["SILVER_DIR"]

os.makedirs(SILVER_DIR, exist_ok=True)

WRITE_OPTS = dict(compression="snappy")

# Mettre à False pour traiter TOUS les .jsonl du dossier (mode batch).
LATEST_ONLY = os.environ["ONLY_LATEST_JSONL"].lower() in ("1", "true", "yes", "y")


def run_hash(base: str) -> str:
    """Hash du nom de base du run, utilisé comme run_id dans la BDD.
    Déterministe : le même run produira toujours le même identifiant.
    """
    return hashlib.sha256(base.encode()).hexdigest()


def run_ts_from_base(base: str) -> datetime:
    """Extrait le timestamp du nom de base du fichier bronze.

    Attend le motif *_YYYYMMDD_HHMMSS en fin de nom
    (ex: prisoner_dilemma_20260527_215238 -> 2026-05-27 21:52:38).
    """
    import re
    m = re.search(r'(\d{8}_\d{6})$', base)
    if not m:
        raise ValueError(f"Impossible d'extraire un timestamp depuis : {base!r}")
    return datetime.strptime(m.group(1), "%Y%m%d_%H%M%S")


def list_jsonl(folder):
    files = glob.glob(os.path.join(folder, "*.jsonl"))
    if not files:
        raise FileNotFoundError(f"Aucun .jsonl dans {folder}")
    return files


def get_latest_jsonl(folder):
    """Dernier fichier d'après son nommage (tri lexicographique).

    Le motif prisoner_dilemma_YYYYMMDD_HHMMSS est triable directement.
    """
    return max(list_jsonl(folder), key=os.path.basename)


def get_unprocessed_jsonl(jsonl_files, silver_dir):
    """Retourne les fichiers JSONL n'ayant pas encore de Parquet correspondant.

    Critère : absence de <base>_fact_turn_play.parquet dans silver_dir.
    """
    unprocessed = []
    for jf in jsonl_files:
        base = os.path.basename(jf).rsplit(".", 1)[0]
        fact_file = os.path.join(silver_dir, f"{base}_fact_turn_play.parquet")
        if not os.path.exists(fact_file):
            unprocessed.append(jf)
    return unprocessed


def load_existing_agent_keys(silver_dir):
    """Charge le référentiel d'agents depuis les *_dim_agent.parquet existants.
    Garantit la cohérence des clés entières entre tous les runs.
    """
    agent_key: dict[str, int] = {}
    for f in glob.glob(os.path.join(silver_dir, "*_dim_agent.parquet")):
        table = pq.read_table(f, columns=["agent_id", "agent_key"])
        for aid, ak in zip(
            table["agent_id"].to_pylist(),
            table["agent_key"].to_pylist(),
        ):
            agent_key[aid] = ak
    return agent_key


def scan_agents(jsonl_files, existing_keys=None):
    """1re passe : référentiel global des agents (clés cohérentes entre fichiers).

    Étend le mapping existant (existing_keys) avec les nouveaux agents
    rencontrés dans jsonl_files, en assignant des clés à partir du max actuel.
    Léger : on ne lit que les deux ids d'agent par ligne.
    """
    agents: set[str] = set()
    for jf in jsonl_files:
        with open(jf, "rb") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                r = orjson.loads(line)
                agents.add(r["agent1_id"])
                agents.add(r["agent2_id"])

    agent_key: dict[str, int] = dict(existing_keys) if existing_keys else {}
    next_key = max(agent_key.values(), default=0) + 1
    for a in sorted(agents):
        if a not in agent_key:
            agent_key[a] = next_key
            next_key += 1
    return agent_key


def process_jsonl(jsonl_file, agent_key):
    """2e passe (worker) : génère fact + dim_match + dim_agent pour un fichier.

    agent_key est le mapping global construit en 1re passe.
    Les match_id étant des UUID, ils servent directement de clé (pas de
    surrogate -> aucun risque de collision entre fichiers).
    Chaque run produit son propre dim_agent pour conserver l'historique.
    """
    f_match_id, f_agent_key, f_opponent_key = [], [], []
    f_turn, f_played_at = [], []
    f_choice, f_opp_choice = [], []
    f_gain, f_cumulative, f_cooperated = [], [], []
    f_justification = []

    matches = {}  # match_id -> (agent1_id, agent2_id, total_turns)
    agents_in_run: set[str] = set()

    with open(jsonl_file, "rb") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            r = orjson.loads(line)

            mid = r["match_id"]
            a1, a2 = r["agent1_id"], r["agent2_id"]
            turn = r["turn"]
            ts = r["timestamp"]
            k1, k2 = agent_key[a1], agent_key[a2]
            agents_in_run.add(a1)
            agents_in_run.add(a2)

            prev = matches.get(mid)
            matches[mid] = (a1, a2, max(turn, prev[2]) if prev else turn)

            # vue agent 1
            f_match_id.append(mid)
            f_agent_key.append(k1)
            f_opponent_key.append(k2)
            f_turn.append(turn)
            f_played_at.append(ts)
            f_choice.append(r["agent1_choice"])
            f_opp_choice.append(r["agent2_choice"])
            f_gain.append(r["agent1_gain"])
            f_cumulative.append(r["agent1_cumulative_score"])
            f_cooperated.append(1 if r["agent1_choice"] == "C" else 0)
            f_justification.append(r.get("agent1_justification", ""))

            # vue agent 2
            f_match_id.append(mid)
            f_agent_key.append(k2)
            f_opponent_key.append(k1)
            f_turn.append(turn)
            f_played_at.append(ts)
            f_choice.append(r["agent2_choice"])
            f_opp_choice.append(r["agent1_choice"])
            f_gain.append(r["agent2_gain"])
            f_cumulative.append(r["agent2_cumulative_score"])
            f_cooperated.append(1 if r["agent2_choice"] == "C" else 0)
            f_justification.append(r.get("agent2_justification", ""))

    base = os.path.basename(jsonl_file).rsplit(".", 1)[0]
    rid = run_hash(base)
    rts = run_ts_from_base(base)
    n_rows = len(f_match_id)

    # ============ dim_agent (par run) ============
    run_agents = sorted(agents_in_run, key=lambda a: agent_key[a])
    pq.write_table(
        pa.table({
            "agent_key": pa.array([agent_key[a] for a in run_agents], type=pa.int32()),
            "agent_id":  pa.array(run_agents),
        }),
        os.path.join(SILVER_DIR, f"{base}_dim_agent.parquet"),
        **WRITE_OPTS,
    )

    # ============ dim_match ============
    match_list = list(matches.keys())
    pq.write_table(
        pa.table({
            "run_id":      pa.array([rid] * len(match_list)),
            "run_ts":      pa.array([rts] * len(match_list), type=pa.timestamp("us")),
            "match_id":    pa.array(match_list),
            "agent1_key":  pa.array([agent_key[matches[m][0]] for m in match_list], type=pa.int32()),
            "agent2_key":  pa.array([agent_key[matches[m][1]] for m in match_list], type=pa.int32()),
            "total_turns": pa.array([matches[m][2] for m in match_list], type=pa.int32()),
        }),
        os.path.join(SILVER_DIR, f"{base}_dim_match.parquet"),
        **WRITE_OPTS,
    )

    # ============ fact_turn_play ============
    pq.write_table(
        pa.table({
            "run_id":           pa.array([rid] * n_rows),
            "match_id":         pa.array(f_match_id),
            "agent_key":        pa.array(f_agent_key,   type=pa.int32()),
            "opponent_key":     pa.array(f_opponent_key, type=pa.int32()),
            "turn_number":      pa.array(f_turn,        type=pa.int32()),
            "played_at":        pa.array([datetime.fromisoformat(t) for t in f_played_at], type=pa.timestamp("us", tz="UTC")),
            "choice":           pa.array(f_choice),
            "opponent_choice":  pa.array(f_opp_choice),
            "gain":             pa.array(f_gain,        type=pa.int16()),
            "cumulative_score": pa.array(f_cumulative,  type=pa.int32()),
            "cooperated":       pa.array(f_cooperated,  type=pa.int8()),
            "justification":    pa.array(f_justification),
        }),
        os.path.join(SILVER_DIR, f"{base}_fact_turn_play.parquet"),
        **WRITE_OPTS,
    )

    return base, rid, n_rows, len(match_list)


if __name__ == "__main__":
    if LATEST_ONLY:
        all_files = [get_latest_jsonl(BRONZE_DIR)]
    else:
        all_files = list_jsonl(BRONZE_DIR)

    # Mode incrémental : ne traiter que les fichiers sans Parquet correspondant
    jsonl_files = get_unprocessed_jsonl(all_files, SILVER_DIR)

    already_done = len(all_files) - len(jsonl_files)
    if already_done:
        print(f"{already_done} run(s) déjà converti(s) — ignoré(s)")
    if not jsonl_files:
        print("Aucun nouveau fichier à traiter.")
        raise SystemExit(0)

    print(f"{len(jsonl_files)} nouveau(x) fichier(s) à traiter")

    start = time.perf_counter()

    # 1re passe (séquentielle, légère) : référentiel global des agents
    # On étend les clés existantes pour garantir la cohérence entre runs
    existing_keys = load_existing_agent_keys(SILVER_DIR)
    agent_key = scan_agents(jsonl_files, existing_keys)
    new_agents = len(agent_key) - len(existing_keys)
    print(f"dim_agent : {len(agent_key)} agents au total ({new_agents} nouveaux)")

    # 2e passe (parallèle) : un worker par fichier
    num_workers = max(cpu_count() - 1, 1)
    print(f"Utilisation de {num_workers} processus...")

    total_facts = 0
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(process_jsonl, f, agent_key): f for f in jsonl_files}
        for fut in tqdm(as_completed(futures), total=len(futures), desc="JSONL -> Parquet"):
            base, rid, n_facts, n_matches = fut.result()
            total_facts += n_facts
            print(f"  {base}  run_id={rid}  ({n_facts} lignes, {n_matches} matchs)")

    print(f"  fact_turn_play : {total_facts} lignes au total")
    print(f"Terminé en {time.perf_counter() - start:.2f}s")
