"""Tests for Jinja2 context rendering."""

from datetime import date

from qualifire.core.context import QualifireContext


class TestQualifireContext:
    def test_built_in_vars(self):
        ctx = QualifireContext(run_id="test-123")
        variables = ctx.get_variables()
        assert variables["run_id"] == "test-123"
        assert variables["today"] == date.today().isoformat()
        assert variables["ds"] == date.today().isoformat()
        assert variables["ds_nodash"] == date.today().strftime("%Y%m%d")

    def test_render_template(self):
        ctx = QualifireContext(extra_context={"my_param": "hello"})
        result = ctx.render("Value is {{ my_param }}")
        assert result == "Value is hello"

    def test_date_add_filter(self):
        ctx = QualifireContext()
        today = date.today().isoformat()
        result = ctx.render("{{ today | date_add(-7) }}")
        from datetime import timedelta
        expected = (date.today() - timedelta(days=7)).isoformat()
        assert result == expected

    def test_date_format_filter(self):
        ctx = QualifireContext()
        result = ctx.render("{{ today | date_format('%Y%m%d') }}")
        assert result == date.today().strftime("%Y%m%d")

    def test_extra_context_override(self):
        ctx = QualifireContext()
        result = ctx.render("{{ ds }}", extra={"ds": "2024-01-01"})
        assert result == "2024-01-01"
