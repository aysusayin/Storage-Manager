"""
Author: Aysu Sayın
"""
import sys
import os

MAX_PAGE_NUM = 100
MAX_RECORD_NUM = 40
RECORD_SIZE = 26  # Bytes
FULL_PAGE_SIZE = 7 + RECORD_SIZE * MAX_RECORD_NUM  # Bytes
INDEX_INTEGER = 0
MAX_TYPE_NAME_LENGTH = 10
MAX_FIELD_NAME_LENGTH = 8
FILE_NAME_LENGTH = 10
MAX_FIELD_NUM = 6
FULL = 0
EMPTY = 1
NOT_FULL_NOT_EMPTY = 2
DELETED = 3


def FindRecordPlace(type_name, key):
    file_name = ''
    page_id = 0
    record_id = 0
    with open(type_name + '_index', 'rb') as index_file:
        while True:
            temp_key = index_file.read(4)  # key
            if temp_key == b'':
                file_name = 'Record Not Found'
                break
            temp_key = int.from_bytes(temp_key, byteorder='big', signed=True)
            if temp_key == key:
                file_name = index_file.read(FILE_NAME_LENGTH).decode('utf-8').strip()  # file name
                page_id = int.from_bytes(index_file.read(4), byteorder='big')  # page_id
                record_id = int.from_bytes(index_file.read(1), byteorder='big')  # record_id
                break
            else:
                index_file.read(15)
    return [file_name, page_id, record_id]


def FindFieldNum(type_name):
    field_num = 0
    with open('sys_cat', 'rb') as sys_cat:
        while True:
            type = sys_cat.read(10).decode("utf-8").strip()
            if type != type_name:
                if type == '':
                    break
                else:
                    sys_cat.seek(49, 1)
            else:
                field_num = int.from_bytes(sys_cat.read(1), byteorder='big')
                break
    return field_num


def CreateType(type_name, num_of_fields, fields):
    sys_cat = open('sys_cat', 'a+b')
    sys_cat.write(bytes(type_name, 'utf-8') + b" " * (MAX_TYPE_NAME_LENGTH - len(type_name)))  # Type Name
    sys_cat.write(num_of_fields.to_bytes(1, byteorder='big'))  # Number Of Fields
    for field in fields:
        sys_cat.write(
            bytes(field.strip(), 'utf-8') + b" " * (MAX_FIELD_NAME_LENGTH - len(field)))  # Names of the fields
    for i in range(MAX_FIELD_NUM - num_of_fields):
        sys_cat.write(b" " * MAX_FIELD_NAME_LENGTH)  # The null fields are filled with black space
    sys_cat.close()
    type_file = open(type_name, 'w+b')
    # Write the parts in the File Header
    type_file.write(bytes(type_name, 'utf-8') + b" " * (
            FILE_NAME_LENGTH - len(type_name)))  # Previous file name (since this is the first file it is its name)
    type_file.write((0).to_bytes(1, byteorder='big'))  # Number of Pages
    type_file.write(EMPTY.to_bytes(1, byteorder='big'))  # isFull
    type_file.write(bytes('          ', 'utf-8'))  # Name of the Next File
    type_index_file = open(type_name + '_index', 'w+')  # Create index file
    type_file.close()
    type_index_file.close()


def DeleteType(type_name):
    sys_cat_copy = open('sys_cat_copy', 'wb+')
    with open('sys_cat', 'rb') as sys_cat:
        while True:
            type = sys_cat.read(10).decode("utf-8").strip()
            if type != type_name:
                if type == '':
                    break
                else:
                    sys_cat.seek(-10, 1)
                    sys_cat_copy.write(sys_cat.read(59))
            else:
                sys_cat.seek(49, 1)
    sys_cat_copy.close()
    file_list = [type_name]
    type_file = open(type_name, 'rb')
    while True:
        type_file.read(12)
        temp_file = type_file.read(10).decode('utf-8').strip()
        if temp_file == '':
            type_file.close()
            break
        else:
            file_list.append(temp_file)
            type_file.close()
            type_file = open(temp_file, 'rb')

    for file in file_list:
        os.remove(file)
    os.remove(type_name + '_index')
    os.remove('sys_cat')
    os.rename('sys_cat_copy', 'sys_cat')


def ListType():
    type_names = []
    with open('sys_cat', 'rb') as sys_cat:
        while True:
            type_name = sys_cat.read(10).decode("utf-8").strip()
            if type_name == '':
                break
            else:
                type_names.append(type_name)
                sys_cat.seek(49, 1)
    type_names.sort()
    for type_name in type_names:
        output_file.write(type_name + '\n')


def CreateRecord(type_name, fields):
    field_num = len(fields)
    type_file = open(type_name, 'rb+')
    type_file.read(11)
    isFull = int.from_bytes(type_file.read(1), byteorder='big')
    while isFull == FULL:
        next_file_name = type_file.read(FILE_NAME_LENGTH).decode("utf-8").strip()
        if next_file_name == '':
            global INDEX_INTEGER
            while os.path.isFile(str(INDEX_INTEGER)):
                INDEX_INTEGER += 1
            type_file1 = open(str(INDEX_INTEGER), 'rb+')
            type_file.seek(-10, 1)
            type_file.write(bytes(str(INDEX_INTEGER), 'utf-8') + b" " * (
                    FILE_NAME_LENGTH - len(str(INDEX_INTEGER))))  # Next File Name
            type_file1.write(bytes(os.path.basename(type_file.name), 'utf-8') + b" " * (
                    FILE_NAME_LENGTH - len(os.path.basename(type_file.name))))  # Previous file name
            type_file1.write((0).to_bytes(1, byteorder='big'))  # Number of Pages
            type_file.write(EMPTY.to_bytes(1, byteorder='big'))  # isFull
            type_file1.write(bytes('          ', 'utf-8'))  # Next File Name
            type_file.close()
            type_file = type_file1
            INDEX_INTEGER += 1
            break
        else:
            type_file1 = open(next_file_name, 'rb+')
            type_file.close()
            type_file = type_file1
            type_file.seek(11)
            isFull = int.from_bytes(type_file.read(1), byteorder='big')

    count = 1
    type_file.seek(26, 0)  # is_empty of first page
    read_temp = type_file.read(1)
    if read_temp == b'':
        type_file.seek(22, 0)
        # if there is no page in file
        type_file.write((0).to_bytes(4, byteorder='big'))
        type_file.write(EMPTY.to_bytes(1, byteorder='big'))
        type_file.write((0).to_bytes(1, byteorder='big'))
        type_file.write((0).to_bytes(1, byteorder='big'))  # Offset after page header

        type_file.seek(10, 0)
        type_file.write((1).to_bytes(1, byteorder='big'))
        type_file.write(NOT_FULL_NOT_EMPTY.to_bytes(1, byteorder='big'))
        type_file.seek(10, 1)
    else:
        # there are some pages in file
        isEmpty = int.from_bytes(read_temp, byteorder='big')
        while isEmpty == FULL:
            # find a page which is not full
            type_file.read(FULL_PAGE_SIZE - 1)
            isEmpty = type_file.read(1)
            if isEmpty != b'':
                # not eof
                isEmpty = int.to_bytes(isEmpty, byteorder='big')
            else:
                # Create a new page
                type_file.seek(-1 * FULL_PAGE_SIZE, 1)
                page_id = int.from_bytes(type_file.read(4), byteorder='big') + 1
                type_file.seek(FULL_PAGE_SIZE, 1)
                type_file.write(page_id.to_bytes(4, byteorder='big'))
                type_file.write(EMPTY.to_bytes(1, byteorder='big'))
                type_file.write((0).to_bytes(1, byteorder='big'))
                type_file.write((0).to_bytes(1, byteorder='big'))  # Offset after page header

                # increment page num in file header
                type_file.seek(10, 0)
                page_num = int.from_bytes(type_file.read(1), byteorder='big')
                type_file.seek(10, 0)
                type_file.write((page_num + 1).to_bytes(1, byteorder='big'))
                # change is_full in file header
                type_file.write(NOT_FULL_NOT_EMPTY.to_bytes(1, byteorder='big'))
                type_file.seek(count * FULL_PAGE_SIZE + 22 + 5, 0)
                break
            count += 1
        if isEmpty == DELETED:
            current = type_file.tell()
            # increment page num in file header
            type_file.seek(10, 0)
            type_file.write((1).to_bytes(1, byteorder='big'))
            type_file.write(NOT_FULL_NOT_EMPTY.to_bytes(1, byteorder='big'))
            type_file.seek(current)
        type_file.seek(-5, 1)
    temp_place_file = type_file.tell()
    type_file.seek(10, 0)
    page_num = int.from_bytes(type_file.read(1), byteorder='big')
    type_file.seek(temp_place_file)
    page_id = int.from_bytes(type_file.read(4), byteorder='big')
    type_file.seek(2, 1)
    offset_of_next_empty_rec = int.from_bytes(type_file.read(1), byteorder='big')
    type_file.seek(offset_of_next_empty_rec, 1)
    type_file.write((int(offset_of_next_empty_rec / RECORD_SIZE)).to_bytes(1, byteorder='big'))  # id of the record
    type_file.write(FULL.to_bytes(1, byteorder='big'))
    for field in fields:
        type_file.write(field.to_bytes(4, byteorder='big', signed=True))
    for i in range(field_num, MAX_FIELD_NUM):
        type_file.write(bytes('    ', 'utf-8'))
    # increment num of records in page header
    type_file.seek(-28 + -1 * offset_of_next_empty_rec, 1)
    num_of_rec = int.from_bytes(type_file.read(1), byteorder='big')
    type_file.seek(-1, 1)
    type_file.write((num_of_rec + 1).to_bytes(1, byteorder='big'))
    # change next empty rec
    page_header_offset = type_file.tell() - 6
    type_file.seek(2, 1)
    isEmpty1 = int.from_bytes(type_file.read(1), byteorder='big')
    type_file.seek(-2, 1)
    while isEmpty1 == FULL:
        type_file.read(RECORD_SIZE + 1)
        isEmpty1 = type_file.read(1)
        if isEmpty1 == b'':
            break
        else:
            type_file.seek(-2, 1)
            isEmpty1 = int.from_bytes(isEmpty1, byteorder='big')
    new_next_empty = type_file.tell() - page_header_offset - 7
    type_file.seek(page_header_offset + 6, 0)
    type_file.write(new_next_empty.to_bytes(1, byteorder='big'))
    # change is Empty change file is Full
    type_file.seek(-3, 1)
    if num_of_rec + 1 == MAX_RECORD_NUM:
        type_file.write(FULL.to_bytes(1, byteorder='big'))
        if page_num == MAX_PAGE_NUM:
            type_file.seek(11, 0)
            type_file.write(FULL.to_bytes(1, byteorder='big'))
    else:
        type_file.write(NOT_FULL_NOT_EMPTY.to_bytes(1, byteorder='big'))

    index_file_copy = open('index_file_copy', 'wb+')
    check = True
    with open(type_name + '_index', 'rb') as index_file:
        initial = index_file.tell()
        index_file.read(1)
        final = index_file.tell()
        index_file.seek(0, 2)
        eof = index_file.tell()
        if initial == final:
            index_file_copy.write(fields[0].to_bytes(4, byteorder='big', signed=True))
            index_file_copy.write(bytes(os.path.basename(type_file.name), 'utf-8') + b" " * (
                    FILE_NAME_LENGTH - len(os.path.basename(type_file.name))))
            index_file_copy.write(page_id.to_bytes(4, byteorder='big'))
            index_file_copy.write((int(offset_of_next_empty_rec / RECORD_SIZE)).to_bytes(1, byteorder='big'))
        else:
            index_file.seek(0, 0)
            while True:
                temp = index_file.read(4)
                if check and index_file.tell() == eof:
                    index_file_copy.write(fields[0].to_bytes(4, byteorder='big', signed=True))
                    index_file_copy.write(bytes(os.path.basename(type_file.name), 'utf-8') + b" " * (
                            FILE_NAME_LENGTH - len(os.path.basename(type_file.name))))
                    index_file_copy.write(page_id.to_bytes(4, byteorder='big'))
                    index_file_copy.write((int(offset_of_next_empty_rec / RECORD_SIZE)).to_bytes(1, byteorder='big'))
                    break
                if temp == b'':
                    break
                temp = int.from_bytes(temp, byteorder='big', signed=True)
                if check and temp > fields[0]:
                    check = False
                    index_file_copy.write(fields[0].to_bytes(4, byteorder='big', signed=True))
                    index_file_copy.write(bytes(os.path.basename(type_file.name), 'utf-8') + b" " * (
                            FILE_NAME_LENGTH - len(os.path.basename(type_file.name))))
                    index_file_copy.write(page_id.to_bytes(4, byteorder='big'))
                    index_file_copy.write((int(offset_of_next_empty_rec / RECORD_SIZE)).to_bytes(1, byteorder='big'))

                index_file_copy.write(temp.to_bytes(4, byteorder='big', signed=True))
                index_file_copy.write(index_file.read(10))
                index_file_copy.write(index_file.read(4))
                index_file_copy.write(index_file.read(1))

    index_file_copy.close()
    os.remove(type_name + '_index')
    os.rename('index_file_copy', type_name + '_index')
    type_file.close()


def DeleteRecord(type_name, key):
    is_remove = False
    record_place = FindRecordPlace(type_name, key)
    file_name = record_place[0]
    if file_name != 'Record Not Found':
        page_id = record_place[1]
        record_id = record_place[2]
        with open(file_name, 'rb+') as type_file:
            type_file.read(22 + FULL_PAGE_SIZE * page_id + 7 + RECORD_SIZE * record_id + 1)
            type_file.write(DELETED.to_bytes(1, byteorder='big'))
            type_file.seek(22 + FULL_PAGE_SIZE * page_id + 5, 0)
            num_of_rec = int.from_bytes(type_file.read(1), byteorder='big')
            type_file.seek(-1, 1)
            type_file.write((num_of_rec - 1).to_bytes(1, byteorder='big'))
            if num_of_rec - 1 == 0:
                type_file.seek(-2, 1)
                type_file.write(DELETED.to_bytes(1, byteorder='big'))
                type_file.seek(10, 0)
                num_of_pages = int.from_bytes(type_file.read(1), byteorder='big')
                type_file.seek(-1, 1)
                print(file_name + ' page num ' + str(num_of_pages))
                type_file.write((num_of_pages - 1).to_bytes(1, byteorder='big'))
                if num_of_pages - 1 == 0:
                    type_file.seek(11, 0)
                    type_file.write(EMPTY.to_bytes(1, byteorder='big'))
                    if file_name != type_name:
                        type_file.seek(0, 0)
                        prev_file_name = type_file.read(10).decode('utf-8').strip()
                        type_file.read(2)
                        next_file_name = type_file.read(10).decode('utf-8').strip()
                        with open(prev_file_name, 'wb+') as pf:
                            pf.read(12)
                            pf.write(bytes(next_file_name, 'utf-8') + b' ' * (FILE_NAME_LENGTH - len(next_file_name)))
                        if next_file_name != '':
                            with open(next_file_name, 'wb+') as nf:
                                pf.write(
                                    bytes(prev_file_name, 'utf-8') + b' ' * (FILE_NAME_LENGTH - len(prev_file_name)))
                        is_remove = True
                else:
                    type_file.seek(11, 0)
                    type_file.write(NOT_FULL_NOT_EMPTY.to_bytes(1, byteorder='big'))
            else:
                type_file.seek(-2, 1)
                type_file.write(NOT_FULL_NOT_EMPTY.to_bytes(1, byteorder='big'))
                type_file.read(1)
                # if the next empty record is after this deleted record, set this record to be next empty record
                offs = int.from_bytes(type_file.read(1), byteorder='big')
                if offs > record_id * RECORD_SIZE:
                    type_file.seek(-1, 1)
                    type_file.write((record_id * RECORD_SIZE).to_bytes(1, byteorder='big'))
    if is_remove:
        os.remove(file_name)


def UpdateRecord(type_name, key, fields):
    field_num = FindFieldNum(type_name)
    record_place = FindRecordPlace(type_name, key)
    file_name = record_place[0]
    if file_name != 'Record Not Found':
        page_id = record_place[1]
        record_id = record_place[2]
        with open(file_name, 'rb+') as type_file:
            type_file.read(22 + FULL_PAGE_SIZE * page_id + 7 + RECORD_SIZE * record_id + 2)
            for field in fields:
                type_file.write(field.to_bytes(4, byteorder='big', signed=True))


def SearchRecord(type_name, key):
    if os.path.isfile(type_name):
        field_num = FindFieldNum(type_name)
        record_place = FindRecordPlace(type_name, key)
        file_name = record_place[0]
        if file_name != 'Record Not Found':
            page_id = record_place[1]
            record_id = record_place[2]
            with open(file_name, 'rb') as type_file:
                type_file.read(22 + FULL_PAGE_SIZE * page_id + 7 + RECORD_SIZE * record_id + 2)
                for i in range(field_num):
                    output_file.write(str(int.from_bytes(type_file.read(4), byteorder='big', signed=True)) + ' ')
                output_file.write('\n')


def ListRecord(type_name):
    record_list = []
    field_num = FindFieldNum(type_name)
    next_file_name = type_name
    while next_file_name.strip() != '':
        type_file = open(next_file_name, 'rb')
        type_file.read(10)
        page_num = int.from_bytes(type_file.read(1), byteorder='big')
        isFileEmpty = int.from_bytes(type_file.read(1), byteorder='big')
        next_file_name = type_file.read(10).decode('utf-8').strip()
        if isFileEmpty == FULL or isFileEmpty == NOT_FULL_NOT_EMPTY:
            count = 0
            while count < page_num:
                type_file.read(4)
                isPageEmpty = int.from_bytes(type_file.read(1), byteorder='big')

                if isPageEmpty == FULL or isPageEmpty == NOT_FULL_NOT_EMPTY:
                    num_of_rec = int.from_bytes(type_file.read(1), byteorder='big')
                    type_file.seek(1, 1)
                    count1 = 0
                    while count1 < num_of_rec:
                        type_file.seek(1, 1)
                        isRecordEmpty = int.from_bytes(type_file.read(1), byteorder='big')
                        if isRecordEmpty == FULL:
                            count1 += 1
                            field_list = []
                            for i in range(field_num):
                                field_list.append(int.from_bytes(type_file.read(4), byteorder='big', signed=True))
                            for i in range(MAX_FIELD_NUM - field_num):
                                type_file.read(4)
                            record_list.append(field_list)
                        else:
                            type_file.seek(24, 1)
                    count += 1
                else:
                    num_rec = int.from_bytes(type_file.read(1), byteorder='big')
                    type_file.seek(RECORD_SIZE * num_rec + 1, 1)
        type_file.close()
        record_list.sort(key=lambda x: x[0])
        for record in record_list:
            for field in record:
                output_file.write(str(field) + ' ')
            output_file.write('\n')


if __name__ == "__main__":
    input_file = open(sys.argv[1], 'r')
    output_file = open(sys.argv[2], 'w')

    for line in input_file:
        words = line.split()
        if words == []:
            pass
        elif words[1] == 'type':
            # DDL operations
            if words[0] == 'create':
                CreateType(words[2], int(words[3]), words[4:])
            if words[0] == 'delete':
                DeleteType(words[2])
            if words[0] == 'list':
                ListType()
        elif words[1] == 'record':
            # DML Operations
            if words[0] == 'create':
                CreateRecord(words[2], list(map(int, words[3:])))
            if words[0] == 'delete':
                DeleteRecord(words[2], int(words[3]))
            if words[0] == 'list':
                ListRecord(words[2])
            if words[0] == 'update':
                UpdateRecord(words[2], int(words[3]), list(map(int, words[3:])))
            if words[0] == 'search':
                SearchRecord(words[2], int(words[3]))
    input_file.close()
    output_file.close()

