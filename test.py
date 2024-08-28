from Aduplicate.Producer import Producer

p = Producer()

p.send(topic='test', value={"b": 5, "a": 6})
print('ok')