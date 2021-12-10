import urllib.request, urllib.parse, urllib.error
import socket, base64
import json
import ssl
import itertools
import re
import codecs
import sqlalchemy
import pandas as pd 
import sqlite3
import colorama
from colorama import Fore
from tqdm import trange

colorama.init(autoreset=True)
array = ['']
numbers = {}
count = 0
DATABASE_LOCATION = "sqlite:///api_numbers.sqlite"
conn = sqlite3.connect('api_numbers.sqlite')
cursor = conn.cursor()
cursor.executescript('''
DROP TABLE IF EXISTS api_numbers;
''')
conn.close()

count_page = 0 
for pages in itertools.count(9998):

  serviceurl = "http://challenge.dienekes.com.br/api/numbers?page=" + str(pages)

  # Ignorando erros de Certificado SSL
  ctx = ssl.create_default_context()
  ctx.check_hostname = False
  ctx.verify_mode = ssl.CERT_NONE

  # ir tentando até conseguir
  data = None
  while data is None:
    try:
        handler = urllib.request.urlopen(serviceurl)
        parms = dict()
        url = str(serviceurl) + urllib.parse.urlencode(parms)
        print('Recebendo', url)
        uh = urllib.request.urlopen(url, context=ctx)
        data = uh.read().decode()
        print('Recebido', len(data), 'characters', end = "\r")
    except:
        pass

  try:
      js = json.loads(data)
  except:
      js = None

  array = []
  if len(data) <= 14: 
    break
  else:
       # Extraindo os números   
    count_page += 1  
    array.append(js['numbers'])
    #print(array)
    for rows in js:
        api_dict = {
        "numbers": js[rows],
        }          
        api_df = pd.DataFrame(api_dict, columns = ["numbers"])
        engine = sqlalchemy.create_engine(DATABASE_LOCATION)
        conn = sqlite3.connect('api_numbers.sqlite')
        cursor = conn.cursor()

        sql_query = """
        CREATE TABLE IF NOT EXISTS api_numbers(
            numbers VARCHAR(200)
        )
        """

        cursor.execute(sql_query)
        print("Salvando dados", end = "\r")

        api_df.to_sql("api_numbers", engine, if_exists='append', index=False)

        conn.close()
        print("Dados salvos com sucesso", end = "\r")
	  
print("Extração sucedida, começando a manipulação dos dados...")    
conn = sqlite3.connect('api_numbers.sqlite')
cursor = conn.cursor()
cursor.execute('SELECT numbers FROM api_numbers')
array_raw = cursor.fetchall()

# Double check para caso por engano aparecer alguma string...
lista = re.findall(r"\d+\.\d+", str(array_raw))

#Criando a Função sort (ordenação) através do binary search (procura binária) 
def binary_search(arr, val, start, end):

	if start == end:
		if arr[start] > val:
			return start
		else:
			return start+1

	if start > end:
		return start

	mid = (start+end)//2
	if arr[mid] < val:
		return binary_search(arr, val, mid+1, end)
	elif arr[mid] > val:
		return binary_search(arr, val, start, mid-1)
	else:
		return mid

def binary_sort(arr):
	for i in trange(1, len(arr)):
		val = arr[i]
		j = binary_search(arr, val, 0, i-1)
		arr = arr[:j] + [val] + arr[j:i] + arr[i+1:]
	return arr

lista_ordenada = binary_sort(lista)

z = 0
while z == 0:
    try:
        test_API = input(Fore.BLUE + "Api ordenado, deseja enviar para o servidor? (y/n) ")

        if test_API == 'y':
            z = 1
            response = 0
            host = input(str("Qual o endereço do servidor que irá hospedar? "))
            path= '/api'
            username= input(str("Username "))
            password= input(str("Password "))
            print('quack')
            try:
                lines= [
                'GET %s HTTP/1.1' % path,
                'Host: %s' % host,
                'Connection: close',
                ]
                s= socket.socket()
                s.connect((host, 8080))
                f= s.makefile('rwb', bufsize=0)
                f.write('\r\n'.join(lines)+'\r\n\r\n')
                f.close()
                response= f.read()
            except:
                pass

            if response > 0:
                print(Fore.GREEN+"Conectado no servidor...")
            else:
                test_continuar = input(Fore.RED+"Falha na conexão com o servidor, deseja continuar? (y/n)...")
                print(Fore.RESET)
                if test_continuar == "y":
                    pass
                if test_continuar == "n":
                    break
                else:
                    pass
            while z == 1:
                test_envio = input("Deseja enviar o arquivo inteiro ou em páginas? i = inteiro / p = páginas ")
                if test_envio == 'i':
                    z = 2
                    fhand = codecs.open('numbers.json', 'w')
                    fhand.write(" \"`{ ")
                    fhand.write(str(lista_ordenada))
                    fhand.write(" }`\" ")
                    fhand.close()
                    print(Fore.YELLOW + "dados exportados para numbers.json")

                if test_envio == 'p':
                    for page in trange(count_page):
                        pagina = lista_ordenada[(page*100):(100*count_page)]
                        fhand = codecs.open('numbers' + str(page)+ '.json', 'w')
                        fhand.write(" \"`{ ")
                        fhand.write(str(pagina))
                        fhand.write(" }`\" ")
                        fhand.close()
                        print(Fore.YELLOW + "dados exportados para numbers" + str(page)+ ".json")
                    z = 2

                else:
                    continue

        if test_API == 'n':
            break

        else:
            continue
    
    except:
        continue
