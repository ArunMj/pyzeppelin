from zeppelin import ZeppelinClient


zepp = ZeppelinClient(host='localhost', port=8088, auth=None)

print "getting all notebooks"
print zepp.list_notebooks()
