'''
	Реализация функционала работы СУБД
	на уровне файлов

	БД представляется в виде файла,
	который имеет следующую структуру:

	TABLE_NAME = table_name_1
	#SCHEMA
	{
		(attr_name, attr_type, attr_attr),
		(...)
	}
	#BODY
	{
		((attr_val_1, attr_name_1), (attr_val_2, attr_name_2)),
		((...), (...))
	}

	Доступны ограничения: 
		Primary Key, 
		Unique, 
		null (пишется в нижнем регистре)

	ВАЖАНО:
	    - принято решение откзаться от внешних ключей ввиду избежать сильного увеличения кода
	    - прянято решение отказаться от раздела where в операторе select, чтобы уменьшить размер кода
'''


import os
import re



# Имя текущей БД
current_db_name = None


# Расширение файла БД
DB_EXTENSION = '.db'


# Имя временной БД, которая используется как промежуточная
# Для выполнения некоторых операций
DB_TEMP_NAME = 'temp.db'


# Шаблон имени таблицы для регулярных выражений
TABLE_NAME_PATTERN = r'[\w_]+'





class SQL_DB_Exception(Exception):
	'''
		Исключение для отлова ошибок работы с файлами БД
	'''
	
	def __init__(self, message, tmp_db=None):
		global DB_TEMP_NAME

		if tmp_db != None:
			tmp_db.__exit__()
			os.remove(DB_TEMP_NAME)

		super().__init__(message)





def setDB(db_name):
	'''
	    Выбор БД для выполнения запросов

	    db_name - имя БД

	    return None
	'''

	global current_db_name

	if not os.path.isfile(db_name + DB_EXTENSION):
		raise SQL_DB_Exception('БД с именем \'{0}\' не существует !'.format(db_name))
	
	current_db_name = db_name



def serializeAttr(attr):
	'''
		Сериализация атрибутов поля таблицы

		attr - атрибуты поля = {
   		 	     'primary key' : True/False,
	   		 	 'unique'      : True/False,
	   		 	 'null'        : True/False
			   }

		return - сериализованные атрибуты поля,
				 пример: 'pk:1;u:0;n:0'
	'''

	return 'pk:{0};u:{1};n:{2}'.format(
		   int(attr['primary key']), int(attr['unique']), int(attr['null']))



def unserializeAttr(attr):
	'''
		Дисериализация атрибутов поля таблицы

		attr - атрибуты поля = 'pk:0/1;u:0/1;n:0/1'

		return - дисериализованные атрибуты поля,
				 пример: {
	   		 	     'primary key' : True,
		   		 	 'unique'      : False,
		   		 	 'null'        : False
			     }
	'''

	return {
		'primary key' : bool(int(attr[3])),
		'unique'      : bool(int(attr[7])),
		'null'        : bool(int(attr[11]))
	}



def recordParse(record):
	'''
		Парсинг записи таблицы

		record - запись из тела таблицы

		return - атрибуты записи в удобной структуре,
			     пример:
			     record = ((name, vlad), (id, 5), (ord_number, 3255))
			     return = (
			     	{
						'attr_name'  : 'name',
						'value'      : 'vlad'
			     	},
			     	{
						'attr_name'  : 'id',
						'value'      : '5'
			     	},
			     	{
						'attr_name'  : 'ord_number',
						'value'      : '3255'
			     	}
			     )
	'''

	attrs = [attr for attr in record[2:-2].split("), (")]
	result = [{'attr_name':attr.split(', ')[0], 'value':attr.split(', ')[1]} for attr in attrs]
	return tuple(result)



def recordUnparse(struct):
	'''
		Преобразование структуры атрибутов в запись таблицы

		struct - структура с атрибутами

		return - запись тела таблицы,
			     пример:
			     struct = (
			     	{
						'attr_name'  : 'name',
						'value'      : 'vlad'
			     	},
			     	{
						'attr_name'  : 'id',
						'value'      : '5'
			     	},
			     	{
						'attr_name'  : 'ord_number',
						'value'      : '3255'
			     	}
			     )
			     return = ((name, vlad), (id, 5), (ord_number, 3255))
	'''

	result = ', '.join(['('+attr['attr_name']+', '+str(attr['value'])+')' for attr in struct])
	return '('+result+')'



def readTableSchema(table_name):
	'''
		Считать схему из таблицы

		table_name - имя таблицы

		return - схема таблицы
				 [
					 {'name' : 'name_attr',
					  'type' : 'type_attr' (integer/string),
					  'attr' : {
						 'primary key' : True/False,
						 'unique'      : True/False,
						 'null'        : True/False
					  }
					 }, {...}
				 ]
	'''

	global current_db_name

	# Если БД не выбрана
	if current_db_name == None:
		raise SQL_DB_Exception('Не выбрана БД !')

	# Поиск таблицы
	isTableFind = False
	isSchemaRead = False
	with open(current_db_name + DB_EXTENSION, 'r') as db:
		for line in db:
			# Проверка существования таблицы в БД
			if 'TABLE_NAME = ' + table_name == line.strip():
				isTableFind = True
				isSchemaRead = True
			elif isSchemaRead and (line.startswith('\t(')):
				# Чтение схемы
				table_schema = []
				# Убрать лишние пробелы
				line = re.sub(r' ', '', line.strip())
				# Вставить между атрибутами разделитель
				line = re.sub(r'\),\(', '|', line)[1:-1]
				for attr in line.split('|'):
					attr = attr.split(',')
					table_schema.append({
						'name' : attr[0].strip(),
						'type' : attr[1].strip(),
						'attr' : unserializeAttr(attr[2].strip())
					})
				# Схема найдена
				break

	# Если таблица не найдена
	if not isTableFind:
		raise SQL_DB_Exception('Таблица \'{0}\' не существует !'.
							   format(table_name))

	return table_schema



def attrIsInteger(table_schema, attr_name):
	'''
		Проверка: тип поля integer ?

		table_schema - схема таблицы
		attr_name - имя поля

		return - True/False
	'''

	for attr in table_schema:
		if attr['name'] == attr_name and attr['type'] == 'integer':
			return True

	return False



def attrIsUnique(table_schema, attr_name):
	'''
		Проверка: является ли поле уникальным

		table_schema - схема таблицы
		attr_name - имя поля

		return - True/False
	'''

	for attr in table_schema:
		if attr['name'] == attr_name and attr['attr']['unique']:
			return True

	return False



def attrIsNull(table_schema, attr_name):
	'''
		Проверка: может ли поле принимать null

		table_schema - схема таблицы
		attr_name - имя поля

		return - True/False
	'''

	for attr in table_schema:
		if attr['name'] == attr_name and attr['attr']['null']:
			return True

	return False



def attrIsString(table_schema, attr_name):
	'''
		Проверка: тип поля string ?

		table_schema - схема таблицы
		attr_name - имя поля

		return - True/False
	'''

	for attr in table_schema:
		if attr['name'] == attr_name and attr['type'] == 'string':
			return True

	return False



def attrIsPrimaryKey(table_schema, attr_name):
	'''
		Проверка: является ли поле первичным ключом

		table_schema - схема таблицы
		attr_name - имя поля

		return - True/False
	'''

	for attr in table_schema:
		if attr['name'] == attr_name and attr['attr']['primary key']:
			return True

	return False



def createTable(table_name, table_schema):
	'''
		Создание таблицы в текущей БД

		table_name - имя таблицы
		table_schema - схема таблицы:
					   [
					   		{name : 'name_attr',
					   		 type : 'type_attr' (integer/string),
					   		 attr : {
					   		 	'primary key' : True/False,
					   		 	'unique'      : True/False,
					   		 	'null'        : True/False
					   		 }
					   		}, {...}
					   ]
		return None
	'''

	global current_db_name

	# Если БД не выбрана
	if current_db_name == None:
		raise SQL_DB_Exception('Не выбрана БД !')

	with open(current_db_name + DB_EXTENSION, 'r+') as db:
		# Проверка существования таблицы в БД
		for line in db:
			if 'TABLE_NAME = ' + table_name == line.strip():
				raise SQL_DB_Exception('Таблица \'{0}\' уже существует !'.format(
									   table_name))
		# Создание таблицы в БД
		db.seek(0, 2)
		db.write('TABLE_NAME = ' + table_name + '\n#SCHEMA\n{\n\t');
		comma_count = len(table_schema) - 1
		for attr in table_schema:
			db.write('({0}, {1}, {2}){3}'.format(
					 attr['name'], attr['type'], serializeAttr(attr['attr']),
					 ', ' if comma_count > 0 else ''))
			comma_count -= 1
		db.write('\n}\n#BODY\n{\n}\n')



def createDB(db_name):
	'''
		Создание файла БД и установка имени текущей БД

		db_name - имя БД

		return None
	'''

	global current_db_name

	# Проверка существования файла БД
	if os.path.isfile(db_name + DB_EXTENSION):
		raise SQL_DB_Exception('БД \'{0}\' уже существует !'.format(
							   db_name))

	with open(db_name + DB_EXTENSION, 'w'):
		if current_db_name == None:
			current_db_name = db_name



def dropTable(table_name):
	'''
		Удаление таблицы из текущей БД

		table_name - имя удаляемой таблицы

		return None
	'''

	global current_db_name

	# Если БД не выбрана
	if current_db_name == None:
		raise SQL_DB_Exception('Не выбрана БД !')

	# Копирование всех таблиц во временную БД, кроме, удаляемой
	isCopyLine = True
	isTableFind = False
	with open(current_db_name + DB_EXTENSION, 'r') as db,\
	     open(DB_TEMP_NAME, 'w') as tmp_db:
		for line in db:
			# Если достигли удаляемую таблицу
			if 'TABLE_NAME = ' + table_name == line.strip():
				isCopyLine = False
				isTableFind = True
			elif not isCopyLine and (re.search(r'TABLE_NAME = ' + TABLE_NAME_PATTERN, line)):
				isCopyLine = True
			# Не копировать удаляемую таблицу
			if isCopyLine:
				tmp_db.write(line)

	# Переименовываем временную БД в текущую
	os.remove(current_db_name + DB_EXTENSION)
	os.rename(DB_TEMP_NAME, current_db_name + DB_EXTENSION)

	# Если удаляемая таблица не найдена
	if not isTableFind:
		raise SQL_DB_Exception('Таблица \'{0}\' не существует !'.format(table_name))



def dropDB(db_name):
	'''
		Удаление файла БД

		db_name - имя БД

		return None
	'''

	global current_db_name

	# Попытка удалить БД
	try:
		os.remove(db_name + DB_EXTENSION)
	except FileNotFoundError:
		raise SQL_DB_Exception('БД \'{0}\' не существует !'.format(
							   db_name))
	except:
		raise SQL_DB_Exception('Произошёл сбой при удалении БД \'{0}\' !'.format(
							   db_name))

	# Если удалена текущая БД
	if current_db_name == db_name:
		current_db_name = None



def isUniqueValue(table_name, attr_name, attr_value):
	'''
		Проверка уникальности значения атрибута

		table_name - имя таблицы
		attr_name  - имя атрибута
		attr_value - значение атрибута

		return - True, если значение атрибута уникально
				     False, если не уникально
	'''

	global current_db_name

	# Если БД не выбрана
	if current_db_name == None:
		raise SQL_DB_Exception('Не выбрана БД !')

	# Поиск таблицы
	isFindTable = False
	isRead = False
	with open(current_db_name + DB_EXTENSION, 'r') as db:
		for line in db:
			if 'TABLE_NAME = ' + table_name == line.strip():
				isFindTable = True
			elif isFindTable and (line.strip() == '#BODY'):
				isRead = True
			elif isRead and line.strip().startswith('(('):
				# Убрать лишние пробелы
				line = re.sub(r' ', '', line.strip())
				# Вставить между парами разделитель
				line = re.sub(r'\),\(', '|', line)[2:-2]
				for attr in line.split('|'):
					name, value = attr.split(',')
					if (attr_name == name) and (attr_value == value):
						return False
			elif isRead and line.strip() == '}':
				break

	return True



def insert_record(db, table_name, table_schema, values):
	'''
		Вставка записи

		db - файл БД для записи
		table_name - имя таблицы для записи
		table_schema - схема таблицы
		values - вставляемые данные

		return None
	'''

	# Проверка коректности и соответствия значений атрибутов схеме
	for name, value in values.items():
		for attr in table_schema:
			if attr['name'] == name:
				# Проверка допустимости значения NULL
				if not attr['attr']['null'] and (value == 'null'):
					raise SQL_DB_Exception('Таблица \'{0}\' поле \'{1}\' не может быть null !'.format(table_name, name), db)
				# Проверка уникальности значения
				if ((attr['attr']['unique'] or attr['attr']['primary key']) and
				    not isUniqueValue(table_name, name, value)):
					raise SQL_DB_Exception('Таблица \'{0}\' поле \'{1}\' должно быть уникальным !'.format(table_name, name), db)
				# Проверка правильности типов данных
				if attrIsInteger(table_schema, name):
					try:
						value = int(value)
					except:
						if attrIsNull(table_schema, name) and value != 'null':
							raise SQL_DB_Exception('В таблице \'{0}\' поле \'{1}\' может принимать только значение типа integer !'.format(table_name, name), db)
				elif attrIsString(table_schema, name):
					if ((value[0] != '\'' or value[-1] != '\'') and
					    (attrIsNull(table_schema, name) and value != 'null')):
						raise SQL_DB_Exception('В таблице \'{0}\' поле \'{1}\' может принимать только значение типа string !'.format(table_name, name), db)
				# Если все данные верны
				break
		else:
			# Если поле не найдено в таблице
			raise SQL_DB_Exception('Таблица \'{0}\' поле \'{1}\' не существует !'.format(table_name, name), db)

	# Запись значений
	db.write('\t(')
	comma_count = len(values) - 1
	for name, value in values.items():
		db.write('({0}, {1}){2}'.format(name, value,
				 ', ' if comma_count > 0 else ''))
		comma_count -= 1
	db.write(')\n')



def insert(table_name, values):
	'''NOT_READY
		Вставка данных в таблицу текущей БД

		table_name - таблица, в которую происходит добавление
		values - добавляемая запись,
				 пример = {
				 	'age'  : 20,
				 	'name' : 'Вася'
				 }

		return None
	'''

	global current_db_name

	# Если БД не выбрана
	if current_db_name == None:
		raise SQL_DB_Exception('Не выбрана БД !')

	# Считывание схемы таблицы
	table_schema = readTableSchema(table_name)

	# Поиск таблицы, в которую происходит вставка
	# Запись результатов в промежуточную таблицу
	isTableFind = False
	isBodyFind = False
	isInsert = False
	isInsertOk = False
	with open(current_db_name + DB_EXTENSION, 'r') as db,\
	     open(DB_TEMP_NAME, 'w') as tmp_db:
		for line in db:
			# Если нашли заданую таблицу
			if 'TABLE_NAME = ' + table_name == line.strip():
				isTableFind = True
			elif isTableFind and not isInsertOk and re.search(r'TABLE_NAME = ' + TABLE_NAME_PATTERN, line.strip()):
				# Создаём тело таблицы
				tmp_db.write('#BODY\n{\n')
				# Добавить запись
				insert_record(tmp_db, table_name, table_schema, values)
				tmp_db.write('}\n')
				isInsertOk = True
			elif isTableFind and ('#BODY' == line.strip()):
				# Если нашли тело таблицы
				isBodyFind = True
			elif isBodyFind and ('{' == line.strip()):
				# Разрешить добавление записи
				isInsert = True
			elif not isInsertOk and isInsert:
				# Добавляем запись в тело таблицы
				insert_record(tmp_db, table_name, table_schema, values)
				isInsertOk = True
			tmp_db.write(line)
		else:
			# Если это была последняя таблица без тела
			if isTableFind and not isInsertOk:
				# Создаём тело таблицы
				tmp_db.write('#BODY\n{\n')
				# Добавить запись
				insert_record(tmp_db, table_name, table_schema, values)
				tmp_db.write('}\n')

	# Если удаляемая таблица не найдена
	if not isTableFind:
		raise SQL_DB_Exception('Таблица \'{0}\' не существует !'.format(table_name))

	# Переименовываем временную БД в текущую
	os.remove(current_db_name + DB_EXTENSION)
	os.rename(DB_TEMP_NAME, current_db_name + DB_EXTENSION)



def delete(table_name, where):
	'''
		Удаление данных из таблицы текущей БД

		table_name - имя таблицы
		where -	условие в разделе where SQL-запроса
				пример:
				{
					'attr_name' : 'возраст',
					'operator'  : '=',		# допустимые: '=' '<>'
					'value'     : '20'
				}

		return None
	'''

	global current_db_name

	# Считывание схемы таблицы
	table_schema = readTableSchema(table_name)

	isTableFind = False
	isBodyFind = False
	isDeleteRecord = False
	isAttrFind = False
	isAttrFind = False
	# Удаление записей таблицы
	with open(current_db_name + DB_EXTENSION, 'r') as db,\
	     open(DB_TEMP_NAME, 'w') as tmp_db:
		for line in db:
			# Если нашли заданую таблицу
			if 'TABLE_NAME = ' + table_name == line.strip():
				isTableFind = True			
			elif isTableFind and ('#BODY' == line.strip()):
				# Найдено тело таблицы
				isBodyFind = True
			elif isBodyFind and ('{' == line.strip()):
				# Разрешить удалять записи из таблицы
				isDeleteRecord = True
			elif isDeleteRecord and (line.strip() == '}'):
				# Завершить модификацию записей
				isTableFind = False
				isDeleteRecord = False
				isBodyFind = False 
			elif isDeleteRecord:
				# В зависимости от условия в where, удалять записи из таблицы
				if where == None:	
					isAttrFind = True
					continue
				else:
					isWhere = False
					attrs = recordParse(line.strip())
					for attr in attrs:
						if (attr['attr_name'] == where['attr_name']):
							isAttrFind = True
							if ((where['operator'] == '='  and attr['value'] == where['value']) or
								(where['operator'] == '<>' and attr['value'] != where['value'])):
								isWhere = True
								break
					if isWhere:
						continue
			tmp_db.write(line)

	# Если поле в where не было найдено
	if not isAttrFind:
		raise SQL_DB_Exception('В таблице \'{0}\' поля \'{1}\' не существует !'.format(table_name, where['attr_name']))

	# Переименовываем временную БД в текущую
	os.remove(current_db_name + DB_EXTENSION)
	os.rename(DB_TEMP_NAME, current_db_name + DB_EXTENSION)



def update(table_name, set_val, where):
	'''
		Обновление данных в таблице текущей БД

		table_name - имя таблицы
		set_val - устанавливаемое значение поля
				  пример:
				  {
						# Было ... set зарплата *= 20 ...
						'attr_name' : 'зарплата',
						'operator'	: '*=', 			# допустимые '=' '*=' '+=' '-=' '/='
						'dvalue'    : '20'
				  }	  
		where -	условие в разделе where SQL-запроса
				пример:
				{
					'attr_name' : 'возраст',
					'operator'  : '=',		# допустимые: '=' '<>'
					'value'     : '20'
				}

		return None
	'''

	global current_db_name

	# Считывание схемы таблицы
	table_schema = readTableSchema(table_name)

	isTableFind = False
	isBodyFind = False
	isUpdateRecord = False
	isAttrFind = False
	# Обновление записей таблицы
	with open(current_db_name + DB_EXTENSION, 'r') as db,\
	     open(DB_TEMP_NAME, 'w') as tmp_db:
		for line in db:
			# Если нашли заданую таблицу
			if 'TABLE_NAME = ' + table_name == line.strip():
				isTableFind = True			
			elif isTableFind and ('#BODY' == line.strip()):
				# Найдено тело таблицы
				isBodyFind = True
			elif isBodyFind and ('{' == line.strip()):
				# Разрешить обновлять записи из таблицы
				isUpdateRecord = True
			elif isUpdateRecord and (line.strip() == '}'):
				# Завершить модификацию записей
				isTableFind = False
				isUpdateRecord = False
				isBodyFind = False 
			elif isUpdateRecord:
				# В зависимости от условия в where, обновлять записи из таблицы
				isUpdate = False
				attrs = recordParse(line.strip())
				if where != None:
					for attr in attrs:
						if (attr['attr_name'] == where['attr_name']):
							isAttrFind = True
							if ((where['operator'] == '='  and attr['value'] == where['value']) or
								(where['operator'] == '<>' and attr['value'] != where['value'])):
								isUpdate = True
								break
				else:
					isUpdate = True
					isAttrFind = True
				# Если поле в where не было найдено
				if not isAttrFind:
					raise SQL_DB_Exception('В таблице \'{0}\' поля \'{1}\' не существует !'.format(table_name, where['attr_name']), tmp_db)
				if isUpdate:
					if attrIsPrimaryKey(table_schema, set_val['attr_name']):
						raise SQL_DB_Exception('В таблице \'{0}\' поле \'{1}\' является первичным ключом, его обновлять нельзя !'.format(table_name, set_val['attr_name']), tmp_db)
					if attrIsUnique(table_schema, set_val['attr_name']):
						raise SQL_DB_Exception('В таблице \'{0}\' поле \'{1}\' является альтернативным ключом, его обновлять нельзя !'.format(table_name, set_val['attr_name']), tmp_db)
					isSetValFind = False
					for attr in attrs:
						if attr['attr_name'] == set_val['attr_name']:
							isSetValFind = True
							if (set_val['operator'] == '='):
								if attrIsInteger(table_schema, attr['attr_name']):
									# Если поле integer - новое значение должно быть тоже integer
									try:
										set_val['dvalue'] = int(set_val['dvalue'])
									except TypeError:
										if attrIsNull(table_schema, attr['attr_name']) and set_val['dvalue'] != 'null':
											raise SQL_DB_Exception('В таблице \'{0}\' поле \'{1}\' может принимать только значение типа integer !'.format(table_name, set_val['attr_name']), tmp_db)
								elif attrIsString(table_schema, attr['attr_name']):			
									# Если поле string - новое значение должно быть тоже string
									if (attrIsNull(table_schema, attr['attr_name']) and 
										set_val['dvalue'] != 'null' and
									    (set_val['dvalue'][0] != '\'' or set_val['dvalue'][-1] != '\'')):
										raise SQL_DB_Exception('В таблице \'{0}\' поле \'{1}\' может принимать только значение типа string !'.format(table_name, set_val['attr_name']), tmp_db)
								attr['value'] = set_val['dvalue']
							elif set_val['operator'] in ('*=', '+=', '-=', '/='):
								if attrIsString(table_schema, attr['attr_name']):
									raise SQL_DB_Exception('В таблице \'{0}\' к полю \'{1}\' типа string нельзя применять данную операцию !'.format(table_name, set_val['attr_name']), tmp_db)
								elif attrIsInteger(table_schema, attr['attr_name']):
									# Если поле integer - новое значение должно быть тоже integer
									try:
										set_val['dvalue'] = int(set_val['dvalue'])
										attr['value'] = int(attr['value'])
										if set_val['operator'] == '*=':
											attr['value'] *= set_val['dvalue']
										elif set_val['operator'] == '+=':
											attr['value'] += set_val['dvalue']
										elif set_val['operator'] == '-=':
											attr['value'] -= set_val['dvalue']
										elif set_val['operator'] == '/=':
											attr['value'] /= set_val['dvalue']
									except TypeError:
										raise SQL_DB_Exception('В таблице \'{0}\' поле \'{1}\' может принимать только значение типа integer !'.format(table_name, set_val['attr_name']), tmp_db)
							line = '\t' + recordUnparse(attrs) + '\n'
					# Если поле в set не было найдено
					if not isSetValFind:
						raise SQL_DB_Exception('В таблице \'{0}\' поля \'{1}\' не существует !'.format(table_name, set_val['attr_name']), tmp_db)
			tmp_db.write(line)

	# Переименовываем временную БД в текущую
	os.remove(current_db_name + DB_EXTENSION)
	os.rename(DB_TEMP_NAME, current_db_name + DB_EXTENSION)



def select(tables, on):
	'''
		Выборка данных из таблицы текущей БД

		tables - имена таблиц, и списки полей
				 пример:
				 (
					 {
						'table_name' : 'table1',
						'attrs': (
							'table1_attr1',
							'table1_attr2',
							'table1_attr3'
						)
					 },
					 {
						'table_name' : 'table2',
						'attrs': (
							'table2_attr1',
							'table2_attr2',
							'table2_attr3'
						)
					 },
					 { ... }
				 )

		on - условия объединения таблиц
			 пример:
			 (
			 	(table1.attr1, table2.attr1),
			 	( ... )
			 )

		return - результат выборки
				 пример:
				 {
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
				 }
				 причём: все null    -> None, 
				 			 integer -> int(), 
				 			 string  -> str() без кавычек вокруг
	'''

	global current_db_name

	# Результирующая таблица
	result = {
		'schema' : list(),
		'body'   : list()
	}

	# Заполнение схемы
	for table in tables:
		for attr_name in table['attrs']:
			table_name = attr_name.split('.')[0]
			if table_name != table['table_name']:
				raise SQL_DB_Exception('Таблица \'{0}\' не существует !'.format(table_name))
			result['schema'].append(attr_name)
	
	# Заполнение имён выбираемых таблиц
	tmp_tables = []
	for table in tables:
		tmp_tables.append({
			'table_name' : table['table_name'],
			'body' : [],
			'isFind' : False
		})

	# Выборка данных из таблиц
	isBodyFind = False
	isSelectRecord = False
	isTableFind = False
	with open(current_db_name + DB_EXTENSION, 'r') as db:
		for line in db:
			# Если нашли таблицу
			if line.startswith('TABLE_NAME = '):
				isTableFind = False
				table_name = line.split(' = ')[1].strip()
				for i in range(len(tmp_tables)):
					if table_name == tmp_tables[i]['table_name']:
						isTableFind = True		
						tmp_table_name = table_name
						tmp_table_num = i
						tmp_tables[i]['isFind'] = True
						break
			elif isTableFind and ('#BODY' == line.strip()):
				# Найдено тело таблицы
				isBodyFind = True
			elif isBodyFind and ('{' == line.strip()):
				# Разрешить выбирать записи из таблицы
				isSelectRecord = True
			elif isSelectRecord and (line.strip() == '}'):
				# Завершить выборку записей
				isSelectRecord = False
				isBodyFind = False
			elif isSelectRecord:
				# Выборка записей
				attrs = list(recordParse(line.strip()))
				for attr in attrs:
					attr['attr_name'] = tmp_table_name + '.' + attr['attr_name']
				# Проверка правильности полей в tables
				for table in tables:
					if table['table_name'] == tmp_table_name:
						table_attrs = [attr for attr in table['attrs']]
						break
				for table_attr in table_attrs:
					if table_attr not in [attr['attr_name'] for attr in attrs]:
						raise SQL_DB_Exception('В таблице \'{0}\' поле \'{1}\' не существует !'.format(tmp_table_name, table_attr))
				tmp_tables[tmp_table_num]['body'].append(attrs)

	# Проверка найдены ли все таблицы
	for tmp_table in tmp_tables:
		if not tmp_table['isFind']:
			raise SQL_DB_Exception('Таблица \'{0}\' не существует !'.format(tmp_table['table_name']))

	# Выполняем inner join таблиц по условию в on
	if on != None:
		for attr1, attr2 in on:
			table1_name = attr1.split('.')[0]
			attr1_name = attr1
			table2_name = attr2.split('.')[0]
			attr2_name = attr2
			# Проверка совместимости типов полей
			table1_schema = readTableSchema(table1_name)
			table2_schema = readTableSchema(table2_name)
			if (not (attrIsInteger(table1_schema, attr1_name.split('.')[1]) and attrIsInteger(table2_schema, attr2_name.split('.')[1])) and
			    not (attrIsString(table1_schema, attr1_name.split('.')[1])  and attrIsString(table2_schema, attr2_name.split('.')[1]))):
				raise SQL_DB_Exception('Поля \'{0}\' и \'{1}\' имеют разные типы !'.format(attr1_name, attr2_name))
			tmp_table1 = None
			tmp_table2 = None
			# Поиск нужных таблиц
			# from pprint import pprint
			# pprint(tmp_tables)
			for tmp_table in tmp_tables:
				if tmp_table['table_name'] == table1_name:
					tmp_table1 = tmp_table
				elif tmp_table['table_name'] == table2_name:
					tmp_table2 = tmp_table
			# Если какая-то из таблиц не была найдена
			if tmp_table1 == None:
				raise SQL_DB_Exception('Таблица \'{0}\' не существует !'.format(table1_name))
			if tmp_table2 == None:
				raise SQL_DB_Exception('Таблица \'{0}\' не существует !'.format(table2_name))
			# Фильтрация не подходящих условию записей
			body = []
			i1 = 0
			len1 = len(tmp_table1['body'])
			while i1 < len1:
				record1 = tmp_table1['body'][i1]
				for attr in record1:
					if attr['attr_name'] == attr1_name:
						attr1_value = attr['value']
						break
				else:
					raise SQL_DB_Exception('В таблице \'{0}\' поле \'{1}\' не существует !'.format(table1_name, attr1_name))
				i2 = 0
				len2 = len(tmp_table2['body'])
				while i2 < len2:
					record2 = tmp_table2['body'][i2]
					for attr in record2:
						if attr['attr_name'] == attr2_name:
							attr2_value = attr['value']
							break
					else:
						raise SQL_DB_Exception('В таблице \'{0}\' поле \'{1}\' не существует !'.format(table2_name, attr2_name))
					# Если значения полей в таблицах совпали
					if attr1_value == attr2_value:
						body.append(record1 + record2)
					i2 += 1
				i1 += 1
			tmp_table1['body'] = body
			tmp_table2['body'] = body
	
	# Оставляем только результирующие записи	
	if on == None:
		body = tmp_tables[0]['body']
	else:		
		for tmp_table in tmp_tables:
			if tmp_table['table_name'] == table2_name:
				body = tmp_table['body']
				break

	# Фильтрация лишних полей
	for record in body:
		i = 0
		count_attrs = len(record)
		while i < count_attrs:
			if record[i]['attr_name'] not in result['schema']:
				del record[i]
				i -= 1
				count_attrs -= 1
			else:
				i += 1

	# Преобразование значений для python
	for record in body:
		new_record = [None for i in range(len(result['schema']))]
		for attr in record:
			if attr['value'] == 'null':
				attr_value = None
			elif attr['value'][0] == '\'' and attr['value'][-1] == '\'':
				attr_value = attr['value'][1:-1]
			else:
				attr_value = int(attr['value'])
			new_record[result['schema'].index(attr['attr_name'])] = attr_value
		result['body'].append(tuple(new_record))
	
	return result
		