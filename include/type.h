//
// Created by 2h 0x on 2021/8/17.
//

#ifndef SIMPLEDATABASE_TYPE_H
#define SIMPLEDATABASE_TYPE_H
namespace db::type
{
    enum class PrepareResult
    {
        PREPARE_SUCCESS,
        PREPARE_SYNTAX_ERROR,
        PREPARE_UNRECOGNIZED_STATEMENT,
    };

    enum class MetaCommandResult
    {
        META_COMMAND_SUCCESS,
        META_COMMAND_UNRECOGNIZED_COMMAND,
    };

    enum class StatementType
    {
        STATEMENT_INSERT,
        STATEMENT_SELECT,
    };

    enum class ExecuteResult
    {
        EXECUTE_SUCCESS,
        EXECUTE_TABLE_FULL,
    };

    struct InputBuffer
    {
        char *buffer;
        size_t buffer_length;
        ssize_t input_length;
    };

    struct Statement
    {
        StatementType type;
        table::Row row_to_insert;
    };
}
#endif //SIMPLEDATABASE_TYPE_H
