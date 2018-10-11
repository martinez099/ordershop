import uuid


class Entity(object):
    """
    Base Entity
    """

    def __init__(self):
        self.id = str(uuid.uuid4())


class Repository(object):
    """
    Repository
    """

    def __init__(self):
        self.items = {}

    def set_item(self, _item):
        try:
            self.items[_item.id] = _item
        except KeyError:
            return False
        return True

    def get_item(self, _id):
        try:
            return self.items[_id]
        except KeyError:
            return False

    def get_items(self):
        return list(self.items.values())

    def del_item(self, _id):
        try:
            item = self.items[_id]
            del self.items[_id]
            return item
        except KeyError:
            return False

    def reset(self):
        self.items = {}
