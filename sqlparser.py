'''
    Реализация парсера SQL запросов

    Синтаксис запросов:
        -- Пробельные символы не влияют на парсинг
        -- Имена БД, таблиц, полей - регистрозависимы
        -- SQL-код регистронезависим
        -- Все строковые значение записываются в одинарных кавычках: 'this is a string'
        -- Ограничение: разделы WHERE могут иметь только одно условие
        --              и допустимы операции: =, <>


        -- СОЗДАНИЕ БД
        CREATE DATABASE database_name_1


        -- УДАЛЕНИЕ БД
        DROP DATABASE database_name_1


        -- СОЗДАНИЕ ТАБЛИЦЫ
        -- Если [null|not_null] не указан, то поумолчанию not_null
        CREATE TABLE table_name_1 (
            attr_name_1 type_name_1{integer|string} [null|not_null] [primary_key|unique],
            ...
        )


        -- УДАЛЕНИЕ ТАБЛИЦЫ
        DROP TABLE table_name_1


        -- ВСТАВКА ЗАПИСИ В ТАБЛИЦУ
        -- Важное замечание: нельзя использовать только VALUES, опуская имена аттрибутов в скобках
        INSERT INTO table_name_1
                   (attr_name_1,  attr_name_2,  ...)
            VALUES (attr_value_1, attr_value_2, ...)


        -- УДАЛЕНИЕ ЗАПИСЕЙ
        -- Если опустить WHERE, все записи в таблице будут удалены
        DELETE FROM table_name_1 
            [WHERE attr_name_1 {=|<>} value_1]


        -- ОБНОВЛЕНИЕ ЗАПИСЕЙ
        -- Если опустить WHERE, все записи в таблице будут обновлены
        -- Ограничение: раздел SET может иметь только 1 поле,
                        допустимые операции: =, *=, +=, /=, -= (умножить, увеличить... на какое-то число)
                        для string только =
                        для integer все
        UPDATE table_name_1 
            SET attr_name_1 += value_1
            [WHERE attr_name2 = value_2]


        -- ВЫБОРКА ЗАПИСЕЙ
        -- Допускается множественое объединение таблиц
        -- Имена полей записываются ввиде: table_name_1.attr_name_1
        -- Ограничение: раздела WHERE нет
        SELECT table_name1.attr_name_1, 
               table_name1.attr_name_2,
               table_name2.attr_name_2,
               ...
        FROM table_name_1
        [INNER JOIN table_name_2 
            ON table_name_1.attr_name_2 = table_name_2.attr_name_2]
'''



import re
import sqldb
from sqldb import SQL_DB_Exception





class SQL_PARSER_Exception(Exception):
    '''
        Исключение для отлова ошибок парсинга SQL
    '''
    
    ...





def parse(sql_code):
    '''
        Парсинг SQL запросов

        sql_code - SQL запросы, разделённые ;
                   пример:
                   CREATE DATABASE school;
                   CREATE TABLE student (
                        ...
                   );
                   INSERT INTO school
                               (...)
                        VALUES (...);

        return - None во всех случаях,
                      кроме SELECT:
                      накапливается список результатов выборки, пример:
                      [{
                            'schema' : [
                                table1_attr1_name,
                                table1_attr2_name, 
                                table2_attr2_name
                            ]
                            'body' : [
                                (table1_attr1_val1, table1_attr2_val1, table2_attr2_val1),
                                (table1_attr1_val2, table1_attr2_val2, table2_attr2_val2),
                                ( ... )
                            ]
                        },
                        { ... }
                      ]
    '''

    # Разбиение всего SQL-кода на отдельные запросы
    querys = []
    i_cur = 0       
    i_prev = 0
    isQuoteOpen = False
    sql_code = sql_code.strip()
    if sql_code == '':
        raise SQL_PARSER_Exception("Ожидался SQL код !")
    for i_cur in range(len(sql_code)):
        if sql_code[i_cur] == "'":
            isQuoteOpen = not isQuoteOpen
        if not isQuoteOpen:
            if sql_code[i_cur] == ';':
                query = sql_code[i_prev:i_cur].strip()
            elif i_cur == len(sql_code)-1:
                query = sql_code[i_prev:i_cur+1].strip()
            else:
                continue
            querys.append(query)
            i_prev = i_cur + 1

    if isQuoteOpen:
        raise SQL_PARSER_Exception('Фатальная ошибка в SQL запросе: незакрытая "\'" !')

    # Выполнение запросов
    result = None
    for query in querys:
        # Определение типа запроса
        if query == '':
            raise SQL_PARSER_Exception('Фатальная ошибка в SQL запросе: лишния ";" !')
        query_cmd = query.split()[0].lower()
        if query_cmd == 'create':
            # Создание БД или таблицы
            query_cmd = query.split()[1].lower()
            if query_cmd == 'database':
                createDB(query)
            elif query_cmd == 'table':
                createTable(query)
            else:
                raise SQL_PARSER_Exception("Неизвестная команда SQL '{0}' !".format(query.split()[1]))
        elif query_cmd == 'drop':
            # Удаление БД или таблицы
            query_cmd = query.split()[1].lower()
            if query_cmd == 'database':
                dropDB(query)
            elif query_cmd == 'table':
                dropTable(query)
            else:
                raise SQL_PARSER_Exception("Неизвестная команда SQL '{0}' !".format(query.split()[1]))
        elif query_cmd == 'insert':
            insert(query)
        elif query_cmd == 'delete':
            delete(query)
        elif query_cmd == 'update':
            update(query)
        elif query_cmd == 'select':
            if result == None:
                result = []
            result.append(select(query))
        else:
            raise SQL_PARSER_Exception("Неизвестная команда SQL '{0}' !".format(query.split()[0]))

    return result



def isNameOk(attr_name):
    '''
        Проверка допустимости имени
        допустимое имя: [A-z_][\d\w_]*

        attr_name - имя поля

        return - True, если имя допустимо, иначе - False
    '''

    pattern = r'[A-z_][\d\w_]*'
    return bool(re.match(pattern, attr_name))



def setDB(db_name):
    '''
        Выбор БД для выполнения запросов

        db_name - имя БД

        return None
    '''
    
    sqldb.setDB(db_name)



def createDB(query):
    '''
        -- СОЗДАНИЕ БД
        CREATE DATABASE database_name_1

        query - запрос на создание БД

        return None
    '''

    # Разбить запрос на части
    # И убрать лишние пробельные символы
    try:
        _, database, database_name = [token.strip() for token in query.split()]
    except ValueError:
        raise SQL_PARSER_Exception("Неправильный синтаксис команды SQL '{0}' !".format(query))

    if database.lower() == 'database':
        # Проверка допустимости имени
        if not isNameOk(database_name):
            raise SQL_PARSER_Exception("Недопустимое имя БД '{0}' !".format(database_name))
        # Выполнить запрос
        sqldb.createDB(database_name)
    else:
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(database))



def dropDB(query):
    '''
        -- УДАЛЕНИЕ БД
        DROP DATABASE database_name_1

        query - запрос на удаление БД

        return None
    '''

    # Разбить запрос на части
    # И убрать лишние пробельные символы
    try:
        _, database, database_name = [token.strip() for token in query.split()]
    except ValueError:
        raise SQL_PARSER_Exception("Неправильный синтаксис команды SQL '{0}' !".format(query))

    if database.lower() == 'database':
        # Проверка допустимости имени
        if not isNameOk(database_name):
            raise SQL_PARSER_Exception("Недопустимое имя БД '{0}' !".format(database_name))
        # Выполнить запрос
        sqldb.dropDB(database_name)
    else:
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(database))



def createTable(query):
    '''
        -- СОЗДАНИЕ ТАБЛИЦЫ
        -- Если [null|not_null] не указан, то поумолчанию not_null
        CREATE TABLE table_name_1 (
            attr_name_1 type_name_1{integer|string} [null|not_null] [primary_key|unique],
            ...
        )

        query - запрос на создание таблицы

        return None
    '''
    
    # Разбить запрос на части:
    # До скобок(заголовок), и в скобках(тело)
    try:
        header, body = [token.strip() for token in query.split("(")]
    except ValueError:
        raise SQL_PARSER_Exception("Неправильный синтаксис команды SQL '{0}' !".format(query))

    # Проверить заголовок
    try:
        _, table, table_name = [token.strip() for token in header.split()]
    except ValueError:
        raise SQL_PARSER_Exception("Неправильный синтаксис команды SQL '{0}' !".format(query))
    if table.lower() != 'table':
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(table))
    # Проверка допустимости имени
    if not isNameOk(table_name):
        raise SQL_PARSER_Exception("Недопустимое имя таблицы '{0}' !".format(table_name))

    # Парсинг тела таблицы
    table_schema = []

    body = [line.strip() for line in body[:-1].split(',') if line.strip() != '']
    if body == []:
        raise SQL_PARSER_Exception("Схема таблицы '{0}' не может быть пустой !".format(table_name))

    for line in body:   
        # Разбиваем строку на части
        # 2 первые части: имя поля, тип
        isOtherEmpty = False
        attr_name, attr_type, *other = [token.strip() for token in line.split()]
        attr_type = attr_type.lower()
        # Проверка допустимости имени
        if not isNameOk(attr_name):
            raise SQL_PARSER_Exception("Недопустимое имя поля '{0}' !".format(attr_name))
        # Проверка совпадения имён полей
        for attr in table_schema:
            if attr['name'] == attr_name:
                raise SQL_PARSER_Exception("Повторное использование имени поля '{0}' !".format(attr_name))
        # Проверка правильности типов данных
        if attr_type != 'integer' and attr_type != 'string':
            raise SQL_PARSER_Exception("Несуществующий тип '{0}' поля '{1}' !".format(attr_type, attr_name))

        isNull = False
        isPrimaryKey = False
        isUnique = False
        if other != []:
            # Парсинг остальной части поля
            for token in other:
                token = token.strip().lower()
                if token == 'not_null':
                    continue
                elif token == 'null':
                    isNull = True
                elif token == 'primary_key':
                    isPrimaryKey = True
                elif token == 'unique':
                    isUnique = True
                else:
                    raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(token))
            # Проверка коректности атрибутов поля
            # Primary Key, Unique не могут быть Null
            if isNull and (isUnique or isPrimaryKey):
                raise SQL_PARSER_Exception('Ошибка в синтаксисе SQL, потенцильный ключ не может быть null !')   

        # Добавить поле в схему
        table_schema.append({
            'name' : attr_name,
            'type' : attr_type,
            'attr' : {
                'primary key' : isPrimaryKey,
                'unique'      : isUnique,
                'null'        : isNull
            }
        })

    # Выполнить запрос
    sqldb.createTable(table_name, table_schema)



def dropTable(query):
    '''
        -- УДАЛЕНИЕ ТАБЛИЦЫ
        DROP TABLE table_name_1

        query - запрос на удаление таблицы

        return None
    '''

    # Разбить запрос на части
    # И убрать лишние пробельные символы
    try:
        _, table, table_name = [token.strip() for token in query.split()]
    except ValueError:
        raise SQL_PARSER_Exception("Неправильный синтаксис команды SQL '{0}' !".format(query))

    if table.lower() == 'table':
        # Проверка допустимости имени
        if not isNameOk(table_name):
            raise SQL_PARSER_Exception("Недопустимое имя '{0}' !".format(table_name))
        # Выполнить запрос
        sqldb.dropTable(table_name)
    else:
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(table))



def insert(query):
    '''
        -- ВСТАВКА ЗАПИСИ В ТАБЛИЦУ
        -- Важное замечание: нельзя использовать только VALUES, опуская имена аттрибутов в скобках
        INSERT INTO table_name_1
                   (attr_name_1,  attr_name_2,  ...)
            VALUES (attr_value_1, attr_value_2, ...)

        query - запрос добавление записи

        return None
    '''

    # Разбить запрос на части
    pattern = r'^\s*(\w{6})\s+(\w{4})\s+(\S+)\s*(\(.+\))\s*(\w{6})\s*(\(.+\))\s*$'
    try:
        _, into, table_name, attr_names, values, attr_values = re.findall(pattern, query, re.I | re.M)[0]
    except:
        raise SQL_PARSER_Exception("Неправильный синтаксис команды SQL '{0}' !".format(query))

    if into.lower() != 'into':
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(into))

    if values.lower() != 'values':
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(values))

    if not isNameOk(table_name):
        raise SQL_PARSER_Exception("Недопустимое имя таблицы '{0}' !".format(table_name))    

    # Извлечение имён полей
    if attr_names[0] != '(' or attr_names[-1] != ')':
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, список имён полей должен быть в скобках '{0}' !".format(query))
    attr_names = attr_names[1:-1].strip() + ','
    pattern = r'^(?:\s*\w[\w_\d]*\s*,\s*)+$'
    if not re.search(pattern, attr_names,  re.I | re.M):
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неправильный список имён полей '{0}' !".format(query))
    pattern = r'\b(\w[\w_\d]*)\b'
    attr_names = re.findall(pattern, attr_names,  re.I | re.M)

    # Извлекаем значения полей
    if attr_values[0] != '(' or attr_values[-1] != ')':
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, список значений полей должен быть в скобках '{0}' !".format(query))
    attr_values = attr_values[1:-1].strip() + ','
    pattern = r'^(?:\s*(?:\'.*\'|\w[\w\d_]*)\s*,)+$'
    if not re.search(pattern, attr_values,  re.I | re.M):
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неправильный список значений полей '{0}' !".format(query))
    pattern = r'(\'.*?\'|\w[\w\d_]*)'
    attr_values = re.findall(pattern, attr_values,  re.I | re.M)
    if len(attr_values) != len(attr_names):
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неправильный список значений полей '{0}' !".format(query))

    values = {name:value for name, value in zip(attr_names, attr_values)}

    # Выполнение запроса
    sqldb.insert(table_name, values)



def delete(query):
    '''
        -- УДАЛЕНИЕ ЗАПИСЕЙ
        -- Если опустить WHERE, все записи в таблице будут удалены
        DELETE FROM table_name_1 
            [WHERE attr_name_1 {=|<>} value_1]

        query - запрос на удаление записей

        return None
    '''

    # Разбить запрос на части
    pattern = r'^\s*(\w{6})\s+(\w{4})\s+(\S+)(?:\s+(\w{5})\s+(\S+?)\b\s*(\S+?)\s*(\'.*\'|\w[\w\d_]*))\s*$'
    try:
        _, from_, table_name, where_, attr_name, operator, attr_value = re.findall(pattern, query,  re.I | re.M)[0]
    except (ValueError, IndexError):
        raise SQL_PARSER_Exception("Неправильный синтаксис команды SQL '{0}' !".format(query))

    if from_.lower() != 'from':
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(from_))

    if not isNameOk(table_name):
        raise SQL_PARSER_Exception("Недопустимое имя таблицы '{0}' !".format(table_name))

    # Проверка правильности раздела WHERE
    where = None
    if where_ != '':
        if where_.lower() != 'where':
            raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(where_))
        if not isNameOk(attr_name):
            raise SQL_PARSER_Exception("Недопустимое имя поля '{0}' !".format(attr_name))
        if operator not in ['=', '<>']:
            raise SQL_PARSER_Exception("Несуществующий оператор '{0}' !".format(operator))
        if not re.search(r'^\'.*\'|\w[\w\d_]*$', attr_value, re.I | re.M):
            raise SQL_PARSER_Exception("Недопустимое значение '{0}' !".format(attr_value))
        where = {
            'attr_name' : attr_name,
            'operator'  : operator,
            'value'     : attr_value
        }

    # Выполнить запрос
    sqldb.delete(table_name, where)



def update(query):
    '''
        -- ОБНОВЛЕНИЕ ЗАПИСЕЙ
        -- Если опустить WHERE, все записи в таблице будут обновлены
        -- Ограничение: раздел SET может иметь только 1 поле,
                        допустимые операции: =, *=, +=, /=, -= (умножить, увеличить... на какое-то число)
                        для string только =
                        для integer все
        UPDATE table_name_1 
            SET attr_name_1 += value_1
            [WHERE attr_name2 = value_2]

        query - запрос на удаление записей

        return None
    '''

    # Разбиваем запрос на части
    pattern = r'^\s*(\w{6})\s+(\S+)\s+(\w{3})\s+(\S+?)\b\s*(\S+?)\s*((?:\'.*\'|\S+))(?:\s+(\w{5})\s+(\S+?)\b\s*(\S+?)\s*(\'.*\'|\S+))?\s*$'
    try:
        _, table_name, set_, set_attr_name, set_operator, set_attr_value,\
        where_, where_attr_name, where_operator, where_attr_value = re.findall(pattern, query, re.I | re.M)[0]
    except (ValueError, IndexError):
        raise SQL_PARSER_Exception("Неправильный синтаксис команды SQL '{0}' !".format(query))
    if set_.lower() != 'set':
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(set_))

    if not isNameOk(table_name):
        raise SQL_PARSER_Exception("Недопустимое имя таблицы '{0}' !".format(table_name))

    if not isNameOk(set_attr_name):
        raise SQL_PARSER_Exception("Недопустимое имя поля '{0}' !".format(set_attr_name))

    if set_operator not in ['=', '*=', '+=', '-=', '/=']:
        raise SQL_PARSER_Exception("Несуществующий оператор '{0}' !".format(set_operator))

    if not re.search(r'^\'.*\'|\w[\w\d_]*$', set_attr_value, re.I | re.M):
        raise SQL_PARSER_Exception("Недопустимое значение '{0}' !".format(set_attr_value))

    set_value = {
        'attr_name' : set_attr_name,
        'operator'  : set_operator,
        'dvalue'    : set_attr_value
    }

    # Проверка правильности раздела WHERE
    where = None
    if where_ != '':
        if where_.lower() != 'where':
            raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(where_))
        if not isNameOk(where_attr_name):
            raise SQL_PARSER_Exception("Недопустимое имя поля '{0}' !".format(where_attr_name))
        if where_operator not in ['=', '<>']:
            raise SQL_PARSER_Exception("Несуществующий оператор '{0}' !".format(where_operator))
        if not re.search(r'^\'.*\'|\w[\w\d_]*$', where_attr_value, re.I | re.M):
            raise SQL_PARSER_Exception("Недопустимое значение '{0}' !".format(where_attr_value))
        where = {
            'attr_name' : where_attr_name,
            'operator'  : where_operator,
            'value'     : where_attr_value
        }

    # Выполнить запрос
    sqldb.update(table_name, set_value, where)



def select(query):
    '''
        -- ВЫБОРКА ЗАПИСЕЙ
        -- Допускается множественое объединение таблиц
        -- Имена полей записываются ввиде: table_name_1.attr_name_1
        -- Ограничение: раздела WHERE нет
        SELECT table_name1.attr_name_1, 
               table_name1.attr_name_2,
               table_name2.attr_name_2,
               ...
        FROM table_name_1
        [INNER JOIN table_name_2 
            ON table_name_1.attr_name_2 = table_name_2.attr_name_2]

        query - запрос на удаление записей

        return None
    '''

    try:
        _, *other = [token.strip() for token in query.split()]
        other = ' '.join(other)
    except (ValueError, IndexError):
        raise SQL_PARSER_Exception("Неправильный синтаксис команды SQL '{0}' !".format(query))

    # Парсинг имён выбираемых полей
    attrs = {}
    table_name = ''
    attr_name = ''
    i_prev = 0
    for i_cur in range(len(other)):
        # Если найден раздел FROM, конец отбора полей
        if other[i_cur] == ',' or other[i_cur:i_cur+4].lower() == 'from':
            try:
                attr_name = other[i_prev:i_cur].strip()
                table_name, attr_name = attr_name.split('.')
            except:
                raise SQL_PARSER_Exception("Неправильный список выбираемых полей '{0}' !".format(query))
            if not isNameOk(table_name):
                raise SQL_PARSER_Exception("Недопустимое имя таблицы '{0}' !".format(table_name))
            if not isNameOk(attr_name):
                raise SQL_PARSER_Exception("Недопустимое имя поля '{0}' !".format(attr_name))
            if table_name not in attrs:
                attrs[table_name] = { 
                    'attrs' : [table_name+'.'+attr_name],
                    'isOk'  : False
                }
            else:
                attrs[table_name]['attrs'].append(table_name+'.'+attr_name)
            if other[i_cur] == ',':   
                i_prev = i_cur+1
            else: 
                other = other[i_cur+4:]
                break    
    else:
        raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, отсутсвует раздел FROM !")

    try:
        table_name, *other = [token.strip() for token in other.split()]
        other = ' '.join(other)
    except (ValueError, IndexError):
        raise SQL_PARSER_Exception("Неправильный синтаксис команды SQL '{0}' !".format(query))
    if not isNameOk(table_name):
        raise SQL_PARSER_Exception("Недопустимое имя таблицы '{0}' !".format(table_name))
    if table_name in attrs:
        attrs[table_name]['isOk'] = True

    # Парсинг раздела(ов) INNER JOIN
    on = []
    while len(other) != 0:
        try:
            inner, join, *other = [token.strip() for token in other.split()]
        except (ValueError, IndexError):
            raise SQL_PARSER_Exception("Неправильный синтаксис команды SQL '{0}' !".format(query))
        other = ' '.join(other)
        if inner.lower() != 'inner':
            raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(inner))
        if join.lower() != 'join':
            raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(join))
        
        try:
            table_name, on_, *other = [token.strip() for token in other.split()]
            other = ' '.join(other)
        except (ValueError, IndexError):
            raise SQL_PARSER_Exception("Неправильный синтаксис команды SQL '{0}' !".format(query))
        if not isNameOk(table_name):
            raise SQL_PARSER_Exception("Недопустимое имя таблицы '{0}' !".format(table_name))
        if table_name in attrs:
            attrs[table_name]['isOk'] = True
        if on_.lower() != 'on':
            raise SQL_PARSER_Exception("Ошибка в синтаксисе SQL, неизвестная команда '{0}' !".format(on_))
        try:
            index = other.index('=')
            other = other[:index] + " = " + other[index+1:]
        except (ValueError, IndexError):
            raise SQL_PARSER_Exception("Неизвестный оператор в разделе ON !")
        pattern = r'\s*(\S+)\b\s*(\S+?)\s*(\S+)\b'
        try:
            attr1_name, operator, attr2_name = re.findall(pattern, other, re.I | re.M)[0]
        except (ValueError, IndexError):
            raise SQL_PARSER_Exception("Неправильный синтаксис команды SQL '{0}' !".format(query))
        other = other[other.index(attr2_name)+len(attr2_name):]
        table1_name, attr1_name = attr1_name.split('.')
        if not isNameOk(table1_name):
            raise SQL_PARSER_Exception("Недопустимое имя таблицы '{0}' !".format(table1_name))
        if not isNameOk(attr1_name):
            raise SQL_PARSER_Exception("Недопустимое имя поля '{0}' !".format(attr1_name))
        table2_name, attr2_name = attr2_name.split('.')
        if not isNameOk(table2_name):
            raise SQL_PARSER_Exception("Недопустимое имя таблицы '{0}' !".format(table2_name))
        if not isNameOk(attr2_name):
            raise SQL_PARSER_Exception("Недопустимое имя поля '{0}' !".format(attr2_name))
        on.append((table1_name+'.'+attr1_name, table2_name+'.'+attr2_name))

    for table_name in attrs:
        if not attrs[table_name]['isOk']:
            raise SQL_PARSER_Exception("Неизвестное имя таблицы '{0}' !".format(table_name))
    
    tables = []
    for table_name in attrs:
        tables.append({
            'table_name' : table_name,
            'attrs'      : tuple(attrs[table_name]['attrs'])
        })
    tables = tuple(tables)

    on = None if on == [] else tuple(on)

    # Выполнить запрос
    return sqldb.select(tables, on)
