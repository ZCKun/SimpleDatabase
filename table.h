//
// Created by 2h 0x on 2021/8/17.
//

#ifndef SIMPLEDATABASE_TABLE_H
#define SIMPLEDATABASE_TABLE_H

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

namespace db::table
{
    struct Row
    {
        uint32_t id;
        char username[COLUMN_USERNAME_SIZE];
        char email[COLUMN_EMAIL_SIZE];
    };
    const uint32_t ID_SIZE = size_of_attribute(Row, id);
    const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
    const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
    const uint32_t ID_OFFSET = 0;
    const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
    const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
    const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

    /* 序列化 */
    inline void serialize_row(const Row *source, void *destination)
    {
        std::memcpy(static_cast<char *>(destination) + ID_OFFSET, &(source->id), ID_SIZE);
        std::memcpy(static_cast<char *>(destination) + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
        std::memcpy(static_cast<char *>(destination) + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
    }

    /* 反序列化 */
    inline void deserialize_row(void *source, Row *destination)
    {
        std::memcpy(&(destination->id), static_cast<char *>(source) + ID_OFFSET, ID_SIZE);
        std::memcpy(&(destination->email), static_cast<char *>(source) + EMAIL_OFFSET, EMAIL_SIZE);
        std::memcpy(&(destination->username), static_cast<char *>(source) + USERNAME_OFFSET, USERNAME_SIZE);
    }

    //
#define TABLE_MAX_PAGES 100
    const uint32_t PAGE_SIZE = 4096;
    const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
    const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;
    struct Table
    {
        uint32_t num_rows;
        void *pages[TABLE_MAX_PAGES];
    };

    inline void *row_slot(Table &table, uint32_t row_num)
    {
        auto page_num = row_num / ROWS_PER_PAGE;
        void *page = table.pages[page_num];
        if (page == nullptr) {
            page = table.pages[page_num] = std::malloc(PAGE_SIZE);
        }
        auto row_offset = row_num % ROWS_PER_PAGE;
        auto byte_offset = row_offset * ROW_SIZE;

        return static_cast<char *>(page) + byte_offset;
    }

}
#endif //SIMPLEDATABASE_TABLE_H
