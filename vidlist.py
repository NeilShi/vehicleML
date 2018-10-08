type_list = ['公-', '私-', '物-', '租-']
city = '上-'
vid_list = []


def generate_vid_list():
    for index in range(len(type_list)):
        for i in range(1, 51):
            vid_list.append(city + type_list[index] + str(i))


def get_vid_map():
    generate_vid_list()
    return vid_list




