# taken from @holger, treelib repository
# no treelib code is used

def crc32(data):
    data = bytes(data, 'UTF-8')

    return hex(zlib.crc32(data) & 0xffffffff)  # crc32 returns a signed value, &-ing it will match py3k


def assign_node_id(depth, root, directory):
    # directory has to be a full directory

    return str(str(depth) + '_' + directory).replace(" ", "_") + '+++' + crc32(os.path.join(root, directory))

def get_parent_id(depth, root, directory):
    if depth == 0:
        return 'ROOT'

    # looking for parent directory
    # e.g. /home/user1/mp3/folder1/parent_folder/current_folder
    # get 'parent_folder'

    search_string = os.path.join(root, directory)
    pos2 = search_string.rfind('/')
    pos1 = search_string.rfind('/', 0, pos2)
    parent_dir = search_string[pos1 + 1:pos2]

    parentid = str(current_depth - 1) + '_' + parent_dir.replace(" ", "_") + '+++' + crc32(root)
    
    return parentid