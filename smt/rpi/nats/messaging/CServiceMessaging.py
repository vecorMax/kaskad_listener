# *******************************************************************************************************
# Класс содержит методы для обмена сообщениями через сервер NATS.                                        *
# @author Селетков И.П. 2018 1214.                                                                      *
# *******************************************************************************************************
import asyncio
import logging
import datetime
import psycopg2
from nats.aio.client import Client as NATSClientLibrary
from nats.aio.errors import ErrNoServers


class CServiceMessaging:
    # Путь к конфигурационным настройкам параметров системы
    path = "settings1.ini"

    # ***************************************************************************************************
    # Конструктор объекта.                                                                              *
    # ***************************************************************************************************
    def __init__(self):
        logging.basicConfig(level=logging.DEBUG)
        # Подключение библиотеки NATS
        self.__nc = NATSClientLibrary()
        # Соединение с PostgreSQL Database Server
        self.conn = psycopg2.connect(
            database="kaskad_demo",
            user="postgres",
            password="docker",
            host="192.168.1.104"
        )
        self.__connect()

    # ***************************************************************************************************
    # Подключение к серверу NATS + оформление подписки на получение сообщений от пользователей          *
    # ***************************************************************************************************
    async def __connect(self):
        if not self.__nc.is_connected:
            logging.info(str(datetime.datetime.now()) + " Establishing connection to NATS server.")
            try:
                await self.__nc.connect("192.168.1.104", loop=asyncio.get_running_loop())
                logging.info(str(datetime.datetime.now()) + " Connection to NATS server is established.")
            except ErrNoServers as e:
                logging.error(str(datetime.datetime.now()) + " Cannot connect to NATS server.", e)

    # ***************************************************************************************************
    # Отправка сообщения на сервера NATS и POSTGRESQL.                                                  *
    # ***************************************************************************************************
    async def send(self, message):
        await self.__connect()
        if not self.__nc.is_connected:
            return
        try:
            # Publishing message to NATS Server by subscription
            await self.__nc.publish("DEMO", bytes(message))
            logging.info(str(datetime.datetime.now()) + " Message was successfully published to NATS server!")
        except Exception as e:
            logging.error(str(datetime.datetime.now()) + " Exception.", e)

    # ***************************************************************************************************
    # Завершение работы, закрытие соединения с сервером.                                                *
    # ***************************************************************************************************
    async def close(self):
        if not self.__nc.is_connected:
            return
        logging.info(str(datetime.datetime.now()) + " Closing connection to NATS server.")
        await self.__nc.close()
        logging.info(str(datetime.datetime.now()) + " Connection to NATS server closed.")

    # ***************************************************************************************************
    # Получение сообщения с сервера NATS.                                                               *
    # ***************************************************************************************************
    async def receive(self):
        await self.__connect()
        if not self.__nc.is_connected:
            return

        async def message_handler(msg):
            try:
                subject = msg.subject
                reply = msg.reply
                data = msg.data
                print(data)
                print()

                # Inserting data to the table of PostgreSQL DB
                cur = self.conn.cursor()
                cur.execute('INSERT INTO kskd_dm (date_load, time_load, reg_load) VALUES(%s, %s, %s)',
                            (datetime.date.today(), datetime.datetime.now().timetz(), int(data)))

                self.conn.commit()
                logging.info(str(datetime.datetime.now()) + " Record inserted successfully into kskd_dm of kaskad_demo's database")
            except Exception as e:
                print(str(datetime.datetime.now()) + 'Exception:', str(e))

        await self.__nc.subscribe("DEMO", cb=message_handler)