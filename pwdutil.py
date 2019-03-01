import base64
import argparse
import pickle
import getpass

""" 
  Trivial Password Obsfucation Utility
  Not really secure at all - key could be protected  or require manual entry to improve this
"""

CONFIG_LOC = "./conf/"
KEY_LOC = "./conf/"

PWD_FILE = CONFIG_LOC + ".pwd"
KEY_FILE = KEY_LOC + ".key"

def get_key(keyfile=None):
    if keyfile != "":
        keyfile = KEY_FILE
    try:
        with open(keyfile) as f:
            key = str(f.readlines())
        return(key)
    except:
        print("Can't locate key {}, exiting".format(keyfile) )
        print("Before you start using this , set up a secret keyfile")
        exit(1)

def encode(key, clear):
    enc = []
    for i in range(len(clear)):
        key_c = key[i % len(key)]
        enc_c = (ord(clear[i]) + ord(key_c)) % 256
        enc.append(enc_c)
    return base64.urlsafe_b64encode(bytes(enc))

def decode(key, enc):
    dec = []
    enc = base64.urlsafe_b64decode(enc)
    for i in range(len(enc)):
        key_c = key[i % len(key)]
        dec_c = chr((256 + enc[i] - ord(key_c)) % 256)
        dec.append(dec_c)
    return "".join(dec)

def store_pwd(encoded, pwdfile):
    print("*** {} ***".format(pwdfile))
    pickle.dump( encoded, open( pwdfile, "wb" ) )
        
def get_pwd(pwdfile):
    encoded = pickle.load( open( pwdfile, "rb" ) )
    return(encoded)
   
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Password Config Util', formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-f', '--file', dest = "filename", default = PWD_FILE, help='Specify encoded filename', action='store')

    group = parser.add_mutually_exclusive_group()
 
    group.add_argument('-s', '--set', dest = "password", help='Store encoded password', action='store_true')
    group.add_argument('-g', '--get', dest = "get", help='Get encoded password', action='store_true')
    #group.add_argument('-f', '--file', dest = "filename", default = PWD_FILE, help='Specify encoded filename', action='store')
    args = vars(parser.parse_args())    
    
    key = get_key()
    
    filename = args["filename"]
    #filename = CONFIG_LOC + filename
    #print(filename)
    
    
    if args["password"]:
        print("Saving Encoded Password:\nPlease enter the password (NOTE - characters will not be echoed to the screen)")
        # Encode Pwd
        password = getpass.getpass('Password:')
        print("Please type the password in again")
        password2 = getpass.getpass('Password:')
        if password == password2:
            encoded = encode(key, password)
            store_pwd(encoded, filename)
        else:
            print("ERROR: passwords don't match")
            exit(1)
    
    elif args["get"]:
        print("Get Encoded Password:")
        # Get from PWD file
        encoded = get_pwd(filename)
        # Decode Pwd
        decoded = decode(key, encoded)
        print(decoded)
    else:
        print("Error - unknown option") 
        parser.print_usage()
        exit(1)   
    
