//
// Created by 2h 0x on 2021/8/17.
//

#ifndef SIMPLEDATABASE_TABLE_H
#define SIMPLEDATABASE_TABLE_H

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
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

    struct Pager
    {
        int file_descriptor;
        uint32_t file_length;
        void* pages[TABLE_MAX_PAGES];
    };

    struct Table
    {
        uint32_t num_rows;
        // void *pages[TABLE_MAX_PAGES];
        Pager pager;
    };

    struct Cursor
    {
        Table table;
        uint32_t row_num;
        bool end_of_table;
    };

    inline Cursor table_start(Table& table)
    {
        Cursor cursor{};
        cursor.table = table;
        cursor.row_num = 0;
        cursor.end_of_table = (table.num_rows == 0);

        return cursor;
    }

    inline Cursor table_end(Table& table)
    {
        Cursor cursor{};
        cursor.table = table;
        cursor.row_num = table.num_rows;
        cursor.end_of_table = true;

        return cursor;
    }

    inline Pager pager_open(const char* filename)
    {
        int fd = open(filename, 
                O_RDWR | // Read/Write mode
                O_CREAT, // Create file if it does not exist
                S_IWUSR | // User write permission
                S_IRUSR // User read permission
            );
        if (fd == -1) {
            printf("Unable to open file\n");
            exit(EXIT_FAILURE);
        }

        off_t file_length = lseek(fd, 0, SEEK_END);
        Pager pager{};
        pager.file_descriptor = fd;
        pager.file_length = file_length;

        for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
            pager.pages[i] = nullptr;
        }

        return pager;
    }

    inline void pager_flush(Pager& pager, uint32_t page_num, uint32_t size)
    {
        if (pager.pages[page_num] == nullptr) {
            printf("Tried to flush null page\n");
            exit(EXIT_FAILURE);
        }

        off_t offset = lseek(pager.file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
        if (offset == -1) {
            printf("Error seeking: %d\n", errno);
            exit(EXIT_FAILURE);
        }

        ssize_t bytes_written = write(pager.file_descriptor, pager.pages[page_num], size);
        if (bytes_written == -1) {
            printf("Error writing: %d\n", errno);
            exit(EXIT_FAILURE);
        }
    }

    inline void* get_page(Pager& pager, uint32_t page_num)
    {
        if (page_num > TABLE_MAX_PAGES) {
            printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
            exit(EXIT_FAILURE);
        }

        if (pager.pages[page_num] == nullptr) {
            void* page = std::malloc(PAGE_SIZE);
            uint32_t num_pages = pager.file_length / PAGE_SIZE;

            if (pager.file_length % PAGE_SIZE) {
                num_pages += 1;
            }

            if (page_num <= num_pages) {
                lseek(pager.file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
                ssize_t bytes_read = read(pager.file_descriptor, page, PAGE_SIZE);
                if (bytes_read == -1) {
                    printf("Error reading file: %d\n", errno);
                    exit(EXIT_FAILURE);
                }
            }

            pager.pages[page_num] = page;
        }

        return pager.pages[page_num];
    }

    inline void* cursor_value(Cursor& cursor)
    {
        uint32_t row_num = cursor.row_num;
        auto page_num = row_num / ROWS_PER_PAGE;
        void* page_ptr = get_page(cursor.table.pager, page_num);
        auto row_offset = row_num % ROWS_PER_PAGE;
        auto byte_offset = row_offset * ROW_SIZE;

        return static_cast<char *>(page_ptr) + byte_offset;
    }

    inline void cursor_advance(Cursor& cursor)
    {
        cursor.row_num += 1;
        if (cursor.row_num >= cursor.table.num_rows) {
            cursor.end_of_table = true;
        }
    }

}
#endif //SIMPLEDATABASE_TABLE_H
