def getusage_type(type):
    usage_type = ""
    if type == 1:
        usage_type = "usage"
    if type == 2:
        usage_type ="bundle"
    if type == 3:
        usage_type = "topup"
    if type == 4:
        usage_type = "om"
    if type == 5:
        usage_type = "ec"
    if type == 6:
        usage_type = 'e-rc'
    if type == 7:
        usage_type = 'roaming'

    return usage_type