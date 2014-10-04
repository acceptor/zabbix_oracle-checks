#!/usr/bin/python
# -*- coding: utf8 -*-


#########################################################################
## @__conn_init   инициирует подключение к базе данных, при 
##                невозможности установить коннект корректно прерывается.
## @__keys        разбирает агрументы командной строки.
## @__sql         парсит файл sql запроса, при необходимости заменяет
##                переменную ${VAR} в sql файле на значение аргумента
##                запуска скрипта.
#########################################################################


##
## Для корректной работы скрипта необходимо сущенствование каталога ./requests
## В данном каталоге должны содержаться файлы с sql запросами.
## для запуска скрипта необходимо указать название файла sql запроса из каталога ./requests,
## например так: C:\python33\python.exe "C:\Program Files\zabbix_agents_2.2.0\scripts\ora_checks.py" discovery_tablespace
## при этом содержимое файла discovery_tablespace будет таким: 
##                     select 
##                         tablespace_name
##                     from
##                         dba_tablespaces where contents != 'TEMPORARY'
## Если же в файле с sql запросом имеется переменная вместо которой надо подставить, например имя таблицы, то скрипт выполняется с параметром -r:
## C:\python33\python.exe "C:\Program Files\zabbix_agents_2.2.0\scripts\ora_checks.py" sqlscriptname -r SUSTEM
##
## При этом сожержание sql sqlscriptname будет следующим:
##                     SELECT
##                         SUM(BYTES) AS fAllocatedBytes
##                     FROM
##                         DBA_DATA_FILES WHERE TABLESPACE_NAME='${VAR}'
##
## Пример использования с zabbix aent: 
##                UserParameter=tablespace.name,C:\python33\python.exe "C:\Program Files\zabbix_agents_2.2.0\scripts\ora_checks.py" discovery_tablespace -m {#TABLESPACENAME}
##                UserParameter=tablespace[*],C:\python33\python.exe "C:\Program Files\zabbix_agents_2.2.0\scripts\ora_checks.py" $1 -r $2
##                UserParameter=audittablesize,C:\python33\python.exe "C:\Program Files\zabbix_agents_2.2.0\scripts\ora_checks.py" audit_size
##                UserParameter=oracheck[*],C:\python33\python.exe "C:\Program Files\zabbix_agents_2.2.0\scripts\ora_checks.py" $1
##                UserParameter=memorypool.name,C:\python33\python.exe "C:\Program Files\zabbix_agents_2.2.0\scripts\ora_checks.py" discovery_memorypool -m {#MEMORYPOOL}
##                UserParameter=memorypool[*],C:\python33\python.exe "C:\Program Files\zabbix_agents_2.2.0\scripts\ora_checks.py" $1 -r "$2"
##                UserParameter=pga[*],C:\python33\python.exe "C:\Program Files\zabbix_agents_2.2.0\scripts\ora_checks.py" pga -r "$1"
##
 
import argparse
import os
import sys
import cx_Oracle
 
def __conn_init(request):
	try: conn = cx_Oracle.connect("Имя/Пароль@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP) "
		                       "(HOST=127.0.0.1)0(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=НапишиСвоё)))",mode=cx_Oracle.SYSDBA)
	except:
		print('CONNECT ERR')
		exit(1)
	else:
		cursor = conn.cursor()
		try: cursor.execute(request)
		except:
			print('EXECUTION ERR')
			cursor.close()
			conn.close()
			exit(1)
		else: result = cursor.fetchall()
	cursor.close()
	conn.close()
	return result
 

 
def __keys():
	parser = argparse.ArgumentParser()
 
	parser.add_argument(
			"sql"
			)

	parser.add_argument(
			"-r",
			"--replace",
			required=False,
			action="store",
			dest="replace",
			default=None,
			)

	parser.add_argument(
			"-m",
			"--macros",
			required=False,
			action="store",
			dest="macros",
			default=None,
			)

	args = parser.parse_args()
	return args



def __sql(sql_file, var=None):
	if var:
		sql = open(sql_file, "r").read()
		request = sql.replace("${VAR}", var)
	else:
		request = open(sql_file, "r").read()
	return request



sql_file = os.path.join(os.path.dirname(sys.argv[0]), "requests" ,__keys().sql)

if os.path.isfile(sql_file):

	if __keys().macros:
		macros = __keys().macros
		request = __sql(sql_file)
		result = __conn_init(request)
		print('{\n "data":[')
		s = 1
		for i in result:
			if len(result)==s: print(('\t{"%s" : "%s"}\n ]\n}')%(macros,i[0]))
			else: print(('\t{"%s" : "%s"},')%(macros,i[0]))
			s+=1
		exit(0)

	if __keys().replace:
		request = __sql(sql_file, __keys().replace)
	else:
		request = __sql(sql_file)

	result = __conn_init(request)
	print(result[0][0])

else:
	print(("SCRIPT ERR: %s is not a file or file does not exist")%(sql_file))
	exit(1)

