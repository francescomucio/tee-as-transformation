from sqlglot import exp
from tee.parser.model_decorator import model


@model(table_name="my_auto_table_one")
def auto_table_one():
    q = exp.select("*").from_("my_first_table")
    return q


    # return [{
    #     "id": "1",
    #     "name": "John Doe"
    # }, {
    #     "id": "2",
    #     "name": "Jane Doe"
    # }]