#!/usr/bin/env python
# coding: utf-8

# In[108]:


import psycopg2


# In[131]:


# 1. Функция, создающая структуру БД (таблицы)


# In[132]:


def req_create_table_clients():
    """
    функция, которая создает запрос на создание таблицы clients
    """
    clients = """CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                first_name VARCHAR(60) NOT NULL,
                last_name VARCHAR(100) NOT NULL,
                email VARCHAR(60)
                );
                """
    return clients


# In[133]:


def req_create_table_phones():
    """
    функция, которая создает запрос на создание таблицы phones
    """
    phones = """CREATE TABLE IF NOT EXISTS phones (
                id SERIAL PRIMARY KEY,
                number VARCHAR(12) UNIQUE NOT NULL,
                client_id INTEGER NOT NULL REFERENCES clients(id)
                );
                """
    return phones


# In[134]:


def send_request(request):
    """
    посылает запрос в БД
    """
    data = ''
    conn = psycopg2.connect(database='clients', user='postgres', password='postgres')
    with conn.cursor() as cur:
        cur.execute(request)
        conn.commit()
    conn.close()


# In[135]:


def send_request_with_fetchall(request):
    """
    посылает запрос в БД и возвращает последнюю запись
    """
    data = ''
    conn = psycopg2.connect(database='clients', user='postgres', password='postgres')
    with conn.cursor() as cur:
        cur.execute(request[0], request[1])
        data = cur.fetchall()
        conn.commit()
    conn.close()
    return data


# In[136]:


def create_db_tables():
    """
    фунция, которая создает таблицы clients и phones в БД
    """
    tables_reqs = [req_create_table_clients(), req_create_table_phones()]
    for req in tables_reqs:
        req = create_table_clients()
        send_request(req) 


# In[ ]:


# создадим БД clients
## createdb -U postgres clients


# In[6]:


# создадим таблицы в БД


# In[137]:


#create_db_tables()


# In[ ]:


# 3. Функция, позволяющая добавить телефон для существующего клиента


# In[155]:


def add_phone(phones, client_id):
    """
    функция, которая добавляет телефон в БД 
    """
    for phone in phones:
        req_add_phone = ("""INSERT INTO phones (number, client_id) VALUES (%s, %s) RETURNING id
                    """, (phone, client_id))
        _ = send_request_with_fetchall(req_add_phone)
    print('Добавлен(ы) телефон(ы) ', *phones, 'для клиента client_id=', client_id)


# In[149]:


# 2. Функция, позволяющая добавить нового клиента


# In[156]:


def add_new_client(first_name, last_name, email=None, phones=[]):
    """
    функция, которая добавляет в БД нового клиента
    """
    req_add_client = ("""INSERT INTO clients (first_name, last_name, email) VALUES (%s, %s, %s
    ) RETURNING id;
    """, (first_name, last_name, email))
    added_id = send_request_with_fetchall(req_add_client)
    print(f'Добавлен клиент id = {added_id[0][0]} имя = {first_name} фамилия = {last_name} email = {email} phone = {phones}')
    if len(phones) != 0:
        add_phone(phones, added_id[0][0])


# In[ ]:


# 4. Функция, позволяющая изменить данные о клиенте


# In[175]:


def update_clients_info(client_id, new_first_name=None, new_last_name=None, new_email=None, old_number=None, new_number=None):
    """
    функция, позволяющая изменить данные о клиенте
    """
    # first_name
    if new_first_name:
        req_update_info = ("""UPDATE clients set first_name=%s WHERE id=%s RETURNING id, first_name
    ;
    """, (new_first_name, client_id))
        back_inf = send_request_with_fetchall(req_update_info)
        print('updated first_name = ', back_inf[0][1],', ', 'client_id = ' , back_inf[0][0])
    # last_name
    if new_last_name:
        req_update_info = ("""UPDATE clients set last_name=%s WHERE id=%s RETURNING id, last_name
    ;
    """, (new_last_name, client_id))
        back_inf = send_request_with_fetchall(req_update_info)
        print('updated last_name = ', back_inf[0][1],', ', 'client_id = ' , back_inf[0][0])
    # email
    if new_last_name:
        req_update_info = ("""UPDATE clients set email=%s WHERE id=%s RETURNING id, email
    ;
    """, (new_email, client_id))
        back_inf = send_request_with_fetchall(req_update_info)
        print('updated email = ', back_inf[0][1],', ', 'client_id = ' , back_inf[0][0])
    # реализация изменения номера менно такая, тк одному кленту могут принадлежать несколько номеров
    if old_number and new_number:
        req_update_info = ("""UPDATE phones set number=%s WHERE number=%s RETURNING id, number
    ;
    """, (new_number, old_number))
        back_inf = send_request_with_fetchall(req_update_info)
        print(back_inf)
        print('updated number = ', back_inf[0][1],f'old number = {old_number}, ', 'id = ' , back_inf[0][0])


# In[176]:


# 4. Функция, позволяющая удалить телефон для существующего клиента


# In[257]:


def delete_number(client_id=None, phone=None):
    """
    функция, позволяющая удалить телефон для существующего клиента либо по номеру телефона либо для id
    """
    if client_id:
        #проверяем, что у данного клиента есть телефоны 
        req_phones_exist = ("""SELECT * FROM phones WHERE client_id=%s
        ;
        """, (client_id,))
        phones = send_request_with_fetchall(req_phones_exist)
        if phones:
            req_delete_phones = ("""DELETE FROM phones WHERE client_id=%s RETURNING id, number
            ;
            """, (client_id,))
            back_inf = send_request_with_fetchall(req_delete_phones)
            print('deleted number(s) ',[i[1] for i in back_inf], 'for client_id = ', client_id)
    elif phone:
        req_phone_exist = ("""SELECT * FROM phones WHERE number=%s
        ;
        """, (phone,))
        if send_request_with_fetchall(req_phone_exist):
            req_delete_phone = ("""DELETE FROM phones WHERE number=%s RETURNING id, number, client_id
            ;
            """, (phone,))
            back_inf = send_request_with_fetchall(req_delete_phone)
            print('deleted number ',back_inf[0][1], 'for client_id = ', back_inf[0][2])        


# In[192]:


# 6. Функция, позволяющая удалить существующего клиента


# In[193]:


def delete_client(client_id):
    """
    функция, позволяющая удалить существующего клиента
    """
    #для соблюдения целостности удалим все номера из таблицы phones для данного клиента
    delete_number(client_id)
    req_delete_client = ("""DELETE FROM clients WHERE id=%s RETURNING id
        ;
        """, (client_id,))
    back_inf = send_request_with_fetchall(req_delete_client)
    print('deleted client with client_id =' , back_inf[0][0])


# In[195]:


# 7. Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)


# In[235]:


def search_client(first_name=None, last_name=None, email=None, phone=None):
    selected_inf = None
    selected_phones = []
    if first_name:
        reg_select_info = ("""SELECT * FROM clients WHERE first_name=%s
    ;
    """, (first_name,))
        selected_inf = send_request_with_fetchall(reg_select_info)
    if last_name:
        if selected_inf:
            selected_inf = [i for i in selected_inf if i[2]==last_name]
        else:
            reg_select_info = ("""SELECT * FROM clients WHERE last_name=%s
            ;
            """, (last_name,))
            selected_inf = send_request_with_fetchall(reg_select_info)
            
    if email:
        if selected_inf:
            selected_inf = [i for i in selected_inf if i[3]==email]
        else:
            reg_select_info = ("""SELECT * FROM clients WHERE email=%s
            ;
            """, (email,))
            selected_inf = send_request_with_fetchall(reg_select_info) 
    if phone:
        reg_select_info = ("""SELECT * FROM phones WHERE number=%s
        ;
        """, (phone,))
        selected_phones = send_request_with_fetchall(reg_select_info)
        if selected_phones:
            client_id = selected_phones[0][2]
            if selected_inf:
                selected_inf = [i for i in selected_inf if i[0] == client_id]
            else:
                reg_select_info = ("""SELECT * FROM clients WHERE id=%s
                ;
                """, (client_id,))
                selected_inf = send_request_with_fetchall(reg_select_info)

    if not phone and selected_inf:
        client_ids = [i[0] for i in selected_inf]
        for client_id in client_ids:
            reg_select_info = ("""SELECT * FROM phones WHERE client_id=%s
            ;
            """, (client_id,))
            selected_phones += send_request_with_fetchall(reg_select_info)
    
    if not selected_inf and not selected_phones:
        return f'Клиент имя = {first_name} фамилия = {last_name} email = {email} phone = {phone} не найден'
    return print('client(s) = ', selected_inf,'phone(s) (id, number, client_id)=', selected_phones)



