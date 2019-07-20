# *******************************************************************************************************
# Класс содержит методы для обмена сообщениями через сервер NATS.                                        *
# @author Селетков И.П. 2018 1214.                                                                      *
# *******************************************************************************************************
import asyncio
import logging
import json
import datetime
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
        self.__nc = NATSClientLibrary()

    # ***************************************************************************************************
    # Подключение к серверу NATS + оформление подписки на получение сообщений от пользователей          *
    # ***************************************************************************************************
    async def __connect(self):
        if not self.__nc.is_connected:
            logging.info(str(datetime.datetime.now()) + " Establishing connection to NATS server.")
            try:
                await self.__nc.connect("192.168.1.104", loop=asyncio.get_running_loop())
                logging.info(str(datetime.datetime.now()) + " Connection to NATS server is established.")
                await CServiceMessaging.receive(self)
                logging.info(str(datetime.datetime.now()) + " Created receiver messages from external devices to NATS server")
            except ErrNoServers as e:
                logging.error(str(datetime.datetime.now()) + " Cannot connect to NATS server.", e)

    # ***************************************************************************************************
    # Отправка сообщения на сервер NATS.                                                                *
    # ***************************************************************************************************
    async def send(self, message):
        await self.__connect()
        if not self.__nc.is_connected:
            return
        try:
            # await self.__nc.publish("TEMP_IN_DEVICE_FROM_SERVER", message.encode("UTF-8"))
            logging.info(str(datetime.datetime.now()) + " Publishing the message by subject to NATS server.")
            await self.__nc.publish("DATA_FROM_CONTROLLER", bytes(message))
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
        if not self.__nc.is_connected:
            return

        async def message_handler(msg):
            data = json.loads(msg.data.decode())

            uuid = data['UUID']
            obj_meas = data['ObjectMeasure']
            cur_time = data['CurrentTime']
            delay_temp = data['Delay']

            # CServiceMessaging.change_delay(delay_temp, 0)

            print(data)

        await self.__nc.subscribe("DATA_ON_CONTROLLER", cb=message_handler)