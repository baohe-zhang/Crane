#!/usr/bin/env python3
import json

import random

d = dict()
d["id"] = []
d["age"] = []
d["gender"] = []
for i in range(100000):
    _id = str(i)
    _age = str(random.randrange(1, 88))
    _gender = str(random.choice(['male','female']))
    d["id"].append(_id)
    d["age"].append(_age)
    d["gender"].append(_gender)
    print(_id, _age, _gender)

with open('data.json', 'w') as f:
    json.dump(d, f)
