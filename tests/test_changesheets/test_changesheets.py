# ChangesheetGenerator tests
import pytest


def test_output_filename_root(changesheet):
    assert changesheet.output_filename_root.startswith("test_changesheet_generator-")


def test_add_changesheet_line_item(changesheet, insert_line_item):
    assert len(changesheet.line_items) == 0
    changesheet.add_line_item(insert_line_item)
    assert len(changesheet.line_items) == 1
    assert changesheet.line_items[0].id == "test_id:01234"
    assert changesheet.line_items[
               0].line == "test_id:01234\tinsert\tsome_attribute\tsome_value"


def test_validate_changesheet(changesheet):
    with pytest.raises(NotImplementedError):
        changesheet.validate_changesheet()

# TODO - fix this test so that it doesn't actually write a file
# def test_write_changesheet(changesheet, insert_line_item):
#     open_mock = mock_open()
#     with patch("builtins.open", open_mock):
#         exp_filename = f"{changesheet.output_filename_root}.tsv"
#         changesheet.write_changesheet()
#         open_mock.assert_called_once_with(exp_filename, "w")
#         open_mock().write.assert_called_with("id\taction\tattribute\tvalue")
