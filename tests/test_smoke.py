"""骨架冒烟测试：仅验证包可导入。"""


def test_import_models() -> None:
    from telemetry import models

    assert models.RejectReason.DUPLICATE.value == "duplicate"
