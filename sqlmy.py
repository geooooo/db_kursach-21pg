'''
    Обеспечение прикладного интерфеса
    для пользователей СУБД SQLMY

    Пользователь СУБД может:
        1) Выбрать базу данных - setDB
        2) Исполнить запрос к текущей базе данных - exec
'''


import sqlparser





class SQLMY_Exception(Exception):
    '''
        Исключение для отлова ошибок SQLMY
    '''
    
    ...





def setDB(database_name):
    '''
        Выбор базы данных для выполнения запросов

        database_name - имя базы данных

        return None
    '''
    
    try:
        sqlparser.setDB(database_name)
    except Exception as e:
        raise SQLMY_Exception(e)



def exec(querys):
    '''
        Парсинг SQL запросов

        querys - SQL запросы, разделённые ;
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
                  результат выборки, пример:
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
    try:
        sqlparser.parse(querys)
    except Exception as e:
        raise SQLMY_Exception(e)