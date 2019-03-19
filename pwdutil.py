import base64
import argparse
import pickle
import getpass

""" 
  Trivial Password Obsfucation Utility
  Split into a password / key pair that are obsfucated and need to be combined to create the password 
"""

CONFIG_LOC = "./conf/"
KEY_LOC = "./conf/"

PWD_FILE = ".pwd"
KEY_FILE = ".key"

def get_key(keyfile=None):
    if keyfile == "":
        keyfile = KEY_FILE
    try:
        with open(keyfile) as f:
            key = str(f.readlines())
        return(key)
    except:
        print("Can't locate key {0}, exiting".format(keyfile) )
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
    print("*** Saved Encoded Password to {} ***".format(pwdfile))
    pickle.dump( encoded, open( pwdfile, "wb" ) )
        
def get_pwd(pwdfile):
    encoded = pickle.load( open( pwdfile, "rb" ) )
    return(encoded)
   
    
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description='Password Config Util', formatter_class=argparse.RawTextHelpFormatter)

    group = parser.add_mutually_exclusive_group()
 
    group.add_argument('-s', '--set', dest = "password", help='Store encoded password', action='store_true')
    group.add_argument('-g', '--get', dest = "get", help='Get encoded password', action='store_true')
    #group.add_argument('-f', '--file', dest = "filename", default = PWD_FILE, help='Specify encoded filename', action='store')

    parser.add_argument('-p', '--pfile', dest="pass_filename", default=PWD_FILE, action='store'
                        , help='Specify encoded password filename - EG .pwd_mypasswd_file')
    parser.add_argument('-k', '--kfile', dest="key_filename", default=KEY_FILE, action='store'
                        , help='Specify encoded key filename - EG .pwd_mypkey_file')

    args = vars(parser.parse_args())

    # Naming convention checks

    if (args["pass_filename"][:5]) != ".pwd_":
        print("Please specify a pass-file name that starts with .pwd_ and don't specify the path" )
        print("path set to ", KEY_LOC)
        exit(1)
    if (args["key_filename"][:5]) != ".key_":
        print("Please specify a key-file name that starts with .key_ and don't specify the path")
        print("path set to ", KEY_LOC)
        exit(1)

    if (args["key_filename"][4:]) != (args["pass_filename"][4:]):
        print("Please use same identifier for key file and pass-file name")
        exit(1)

    pass_filename = KEY_LOC + args["pass_filename"]
    key_filename = KEY_LOC + args["key_filename"]
    #print(pass_filename)
    #print(key_filename)

    key = get_key(key_filename)

    if args["password"]:
        print("Saving Encoded Password:\nPlease enter the password (NOTE - characters will not be echoed to the screen)")
        # Encode Pwd
        password = getpass.getpass('Password:')
        print("Please type the password in again")
        password2 = getpass.getpass('Password:')
        if password == password2:
            encoded = encode(key, password)
            store_pwd(encoded, pass_filename)
        else:
            print("ERROR: passwords don't match")
            exit(1)
    
    elif args["get"]:
        print("Get Encoded Password:")
        # Get from PWD file
        encoded = get_pwd(pass_filename)
        # Decode Pwd
        decoded = decode(key, encoded)
        print(decoded)
    else:
        print("Error - unknown option; set [ -s ] or get [ -g ] a password")
        print("\nKey file location set to", KEY_LOC,"\n")
        parser.print_usage()
        exit(1)   
