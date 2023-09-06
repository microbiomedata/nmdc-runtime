# ChangesheetGenerator tests
from unittest.mock import patch, mock_open


def test_output_filename_root(base_changesheet_generator):
    assert base_changesheet_generator.output_filename_root.startswith("test_changesheet_generator-")


def test_add_changesheet_line_item(base_changesheet_generator, insert_line_item):
    assert len(base_changesheet_generator.changesheet.line_items) == 0
    base_changesheet_generator.add_changesheet_line_item(insert_line_item)
    assert len(base_changesheet_generator.changesheet.line_items) == 1
    assert base_changesheet_generator.changesheet.line_items[0].id == "test_id:01234"


def test_validate_changesheet(base_changesheet_generator):
    assert base_changesheet_generator.validate_changesheet() == NotImplemented


def test_write_changesheet(base_changesheet_generator, insert_line_item):
    open_mock = mock_open()
    with patch("builtins.open", open_mock):
        # base_changesheet_generator.add_changesheet_line_item(insert_line_item)
        filename = base_changesheet_generator.output_filename_root + ".tsv"
        base_changesheet_generator.write_changesheet(filename)
        open_mock.assert_called_once_with(filename, "w")
        open_mock().write.assert_called_with("id\taction\tattribute\tvalue")
