from gold.utils import name_csv_file


def test_with_folder_path_prefixes_the_folder() -> None:
    assert name_csv_file("some/folder", "postgres", "type_metal") == "some/folder/postgres_type_metal.csv"


def test_without_folder_path_returns_bare_filename() -> None:
    assert name_csv_file(None, "postgres", "type_metal") == "postgres_type_metal.csv"


def test_custom_extension_is_respected() -> None:
    assert name_csv_file(None, "postgres", "type_metal", extension=".jsonl") == "postgres_type_metal.jsonl"
