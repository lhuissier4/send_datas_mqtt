def test_deliberately_failing_for_branch_protection_validation() -> None:
    assert False, "intentional failure to validate branch protection blocks merge"
