import json
import os.path
from datetime import datetime
from abc import ABC, abstractmethod


class Serializer(ABC):
    @abstractmethod
    def serialize(self, data):
        pass

    @abstractmethod
    def deserialize(self, data):
        pass


class JSONSerializer(Serializer):
    def serialize(self, data):
        return json.dumps(data)

    def deserialize(self, data):
        return json.loads(data)


class Storage:
    def _init_(self, log_filename, serializer: Serializer):
        self.log_filename = log_filename
        self.serializer = serializer

        self.init_log()

    def init_log(self):
        # check for existing file
        if not os.path.exists(self.log_filename):
            with open(self.log_filename, 'w'):
                print("initialized log file in write mode: {}".format(self.log_filename))

    def append_log(self, operation, key, value=None):
        timestamp = datetime.now().isoformat()
        log_entry = {
            "timestamp": timestamp,
            "operation": operation,
            "key": key,
            "value": value
        }

        with open(self.log_filename, 'a') as log_file:
            log_file.write(self.serializer.serialize(log_entry) + "\n")

    def read_log(self):
        if os.path.exists(self.log_filename):
            with open(self.log_filename, 'r') as log_file:
                res = []
                for line in log_file.readlines():
                    res.append(self.serializer.deserialize(line.strip()))
                return res


class Database:
    def _init_(self, storage):
        self.storage = storage
        self.db = {}
        self.recover_from_log()

    def recover_from_log(self):
        log_entries = self.storage.read_log()
        for entry in log_entries:
            key = entry["key"]
            value = entry["value"]
            operation = entry["operation"]
            if operation == "INSERT" or operation == "UPDATE":
                self.db[key] = value

    def insert(self, key, value):
        if key in self.db:
            raise KeyError("Key {} already exists".format(key))

        self.db[key] = value
        self.storage.append_log('INSERT', key, value)

    def update(self, key, value):
        if key not in self.db:
            raise KeyError("Key {} does not".format(key))

        self.db[key] = value
        self.storage.append_log('UPDATE', key, value)

    def select(self, key):
        return self.db.get(key, None)


if _name_ == '_main_':
    serializer = JSONSerializer()
    storage = Storage("db.log", serializer)
    database = Database(storage)

    print("@@ db: ", database.db)