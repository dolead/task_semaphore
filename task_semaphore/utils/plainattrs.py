class PlainAttrs:
    # to be redefined in implementing class
    KEYS_TO_SERIALIZE = []

    def to_plain(self):
        return {key: getattr(self, key)
                for key in self.KEYS_TO_SERIALIZE}

    def from_plain(self, attrs_dict):
        """From a plain dict"""
        for key, val in attrs_dict.items():
            setattr(self, key, val)
