#include <iostream>
#include "table.h"
#include "type.h"
using namespace db::table;
using namespace db::type;

inline void print_prompt()
{
    printf("db > ");
}

inline void print_row(const Row &row)
{
    printf("(%d, %s, %s)\n", row.id, row.username, row.email);
}

inline void read_input(InputBuffer &buffer)
{
    auto bytes_read = getline(&(buffer.buffer), &(buffer.buffer_length), stdin);
    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // -1 is \0
    buffer.input_length = bytes_read - 1;
    buffer.buffer[bytes_read - 1] = 0;
}

inline MetaCommandResult do_meta_command(InputBuffer &buffer)
{
    if (std::strcmp(buffer.buffer, ".exit") == 0) {
        exit(EXIT_SUCCESS);
    }
    return MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND;
}

inline PrepareResult prepare_statement(InputBuffer &input_buffer, Statement &statement)
{
    if (std::strncmp(input_buffer.buffer, "insert", 6) == 0) {
        statement.type = StatementType::STATEMENT_INSERT;
        auto &statement_row = statement.row_to_insert;
        auto args_assigned = std::sscanf(input_buffer.buffer, "insert %d %s %s",
                                         &(statement_row.id), statement_row.username, statement_row.email);
        if (args_assigned < 3) {
            return PrepareResult::PREPARE_SYNTAX_ERROR;
        }
        return PrepareResult::PREPARE_SUCCESS;
    }

    if (std::strncmp(input_buffer.buffer, "select", 6) == 0) {
        statement.type = StatementType::STATEMENT_SELECT;
        return PrepareResult::PREPARE_SUCCESS;
    }

    return PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT;
}

inline ExecuteResult execute_insert(const Statement &statement, Table &table)
{
    if (table.num_rows >= TABLE_MAX_ROWS) {
        return ExecuteResult::EXECUTE_TABLE_FULL;
    }

    auto &row_to_insert = statement.row_to_insert;
    // save to table page
    serialize_row(&row_to_insert, row_slot(table, table.num_rows));
    table.num_rows += 1;

    return ExecuteResult::EXECUTE_SUCCESS;
}

inline ExecuteResult execute_select(const Statement &statement, Table &table)
{
    Row row{};
    for (uint32_t i = 0; i < table.num_rows; i++) {
        deserialize_row(row_slot(table, i), &row);
        print_row(row);
    }
    return ExecuteResult::EXECUTE_SUCCESS;
}

inline ExecuteResult execute_statement(const Statement &statement, Table &table)
{
    switch (statement.type) {
        case StatementType::STATEMENT_INSERT:
            return execute_insert(statement, table);
        case StatementType::STATEMENT_SELECT:
            return execute_select(statement, table);
    }
}

int main()
{
    InputBuffer input_buffer{};
    Table table{};

    while (true) {
        print_prompt();
        read_input(input_buffer);

        if (std::strcmp(input_buffer.buffer, ".exit") == 0) {
            if (input_buffer.buffer[0] == '.') {
                switch (do_meta_command(input_buffer)) {
                    case (MetaCommandResult::META_COMMAND_SUCCESS):
                        continue;
                    case (MetaCommandResult::META_COMMAND_UNRECOGNIZED_COMMAND):
                        printf("Unrecognized command '%s'.\n", input_buffer.buffer);
                        continue;
                }
            }
        }

        Statement statement{};
        switch (prepare_statement(input_buffer, statement)) {
            case (PrepareResult::PREPARE_SUCCESS):
                break;
            case (PrepareResult::PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PrepareResult::PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n", input_buffer.buffer);
                continue;
        }

        switch (execute_statement(statement, table)) {
            case ExecuteResult::EXECUTE_SUCCESS:
                printf("Executed.\n");
                break;
            case ExecuteResult::EXECUTE_TABLE_FULL:
                printf("Error: Table full.\n");
                break;
        }
    }

    return 0;
}
