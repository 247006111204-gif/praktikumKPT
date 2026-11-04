import xmlrpc.client

proxy = xmlrpc.client.ServerProxy("http://localhost:8000/", verbose=True)

print("Daftar method:", proxy.system.listMethods())
print("5 + 3 =", proxy.tambah(5, 3))
print("4 x 7 =", proxy.kali(4, 7))
try:
    print(proxy.bagi(10, 0))
except xmlrpc.client.Fault as e:
    print("Error dari server:", e.faultString)
