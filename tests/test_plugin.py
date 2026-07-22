import ast
import unittest
from pathlib import Path


PLUGIN_PATH = Path(__file__).resolve().parents[1] / "plugin.py"
SOURCE = PLUGIN_PATH.read_text(encoding="utf-8")
TREE = ast.parse(SOURCE)
PURE_NAMES = {
    "normalize_content_text",
    "build_content_fingerprint",
    "content_similarity",
}
NODES = [
    node
    for node in TREE.body
    if isinstance(node, (ast.Import, ast.ImportFrom))
    and any(alias.name in {"hashlib", "difflib", "json", "re", "unicodedata"} for alias in node.names)
    or isinstance(node, ast.FunctionDef) and node.name in PURE_NAMES
]
NAMESPACE = {"Any": object, "List": list, "Tuple": tuple}
exec(compile(ast.Module(body=NODES, type_ignores=[]), str(PLUGIN_PATH), "exec"), NAMESPACE)
normalize_content_text = NAMESPACE["normalize_content_text"]
build_content_fingerprint = NAMESPACE["build_content_fingerprint"]
content_similarity = NAMESPACE["content_similarity"]


class ContentHelpersTest(unittest.TestCase):
    def test_fingerprint_is_stable_across_markers_and_whitespace(self):
        first = build_content_fingerprint(
            "========== 转发消息开始 ==========\nＡ  B\n========== 转发消息结束 ==========",
            [("image", "[图片：猫]")],
        )
        second = build_content_fingerprint("a b", [("image", "[图片：猫]")])
        self.assertEqual(first, second)

    def test_urls_do_not_destabilize_text(self):
        self.assertEqual(normalize_content_text("看 https://a.example/1"), "看 [url]")
        self.assertEqual(normalize_content_text("看 https://b.example/2"), "看 [url]")

    def test_similarity_detects_near_duplicate(self):
        self.assertGreater(content_similarity("群友把数据库删了", "群友把数据库删了！"), 0.9)
        self.assertLess(content_similarity("数据库删了", "今天吃什么"), 0.5)


class SourceContractTest(unittest.TestCase):
    def test_reject_keywords_are_checked_before_pass_keywords(self):
        reject_pos = SOURCE.index("matched_reject")
        pass_pos = SOURCE.index("matched_pass")
        self.assertLess(reject_pos, pass_pos)

    def test_extended_analysis_contract_is_present(self):
        for field in (
            "summary",
            "joke_points",
            "context_dependency",
            "content_tags",
            "risk_tags",
        ):
            self.assertIn(field, SOURCE)

    def test_vlm_is_not_called(self):
        self.assertNotIn("process_image(", SOURCE)
        self.assertNotIn("get_image_description(", SOURCE)
        self.assertIn("translate_pid_to_description", SOURCE)


class SQLiteSchemaTest(unittest.TestCase):
    def test_schema_supports_per_target_delivery(self):
        schema_match = SOURCE[SOURCE.index("CREATE TABLE IF NOT EXISTS jobs"):SOURCE.index('""")', SOURCE.index("CREATE TABLE IF NOT EXISTS jobs"))]
        self.assertIn("CREATE TABLE IF NOT EXISTS deliveries", schema_match)
        self.assertIn("UNIQUE(job_id, target_group_id)", schema_match)
        self.assertIn("CREATE TABLE IF NOT EXISTS content_history", schema_match)
        self.assertIn("CREATE TABLE IF NOT EXISTS cooldowns", schema_match)


if __name__ == "__main__":
    unittest.main()
