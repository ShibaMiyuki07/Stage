def data_usage_global(r):
    data = {}
    data['data'] =  { 
        'sms_i_cnt' : r['sms_i_cnt'],
        'voice_i_cnt' : r['voice_i_cnt'],
        'voice_i_vol' : r['voice_i_vol'],
        'voice_i_amnt' : r['voice_i_amnt'],
        'voice_i_cnt' : r['voice_i_amnt'],
        'voice_o_cnt' : r['voice_o_cnt'],
        'voice_o_main_vol' : r['voice_o_main_vol'],
        'voice_o_amnt' : r['voice_o_amnt'],
        'voice_o_bndl_vol' : r['voice_o_bndl_vol'],
        'sms_o_main_cnt' : r['sms_o_main_cnt'],
        'sms_o_bndl_cnt' : r['sms_o_bndl_cnt'],
        'sms_o_amnt' : r['sms_o_amnt'],
        'data_main_vol' : r['data_main_vol'],
        'data_amnt' : r['data_amnt'],
        'usage_2G' : r['usage_2G'],
        'usage_3G' : r['usage_3G'],
        'usage_4G_TDD' : r['usage_4G_TDD'],
        'usage_4G_FDD' : r['usage_4G_FDD'],
        'data_bndl_vol' : r['data_bndl_vol'],
        'voice_vas_cnt' : r['voice_vas_cnt'],
        'voice_vas_amnt' :r['voice_vas_amnt'],
        'voice_vas_main_vol' : r['voice_vas_main_vol'],
        'voice_vas_bndl_vol' : r['voice_vas_bndl_vol'],
        'sms_vas_cnt' : r['sms_vas_cnt'],
        'sms_vas_bndl_cnt' : r['sms_vas_bndl_cnt'],
        'sms_vas_amnt' : r['sms_vas_amnt']
        }
    return data['data']

def data_daily_usage(r):
    data = {}
    data['data'] = { 
                    'sms_i_cnt' : r['sms_i_cnt'],
                        'voice_i_cnt' : r['voice_i_cnt'],
                        'voice_i_vol' : r['voice_i_vol'],
                        'voice_i_amnt' : r['voice_i_amnt'],
                        'voice_i_cnt' : r['voice_i_amnt'],
                        'voice_o_cnt' : r['voice_o_cnt'],
                        'voice_o_main_vol' : r['voice_o_main_vol'],
                        'voice_o_amnt' : r['voice_o_amnt'],
                        'voice_o_bndl_vol' : r['voice_o_bndl_vol'],
                        'sms_o_main_cnt' : r['sms_o_main_cnt'],
                        'sms_o_bndl_cnt' : r['sms_o_bndl_cnt'],
                        'sms_o_amnt' : r['sms_o_amnt'],
                        'data_main_vol' : r['data_main_vol'],
                        'data_amnt' : r['data_amnt'],
                        'usage_2G' : r['usage_2G'],
                        'usage_3G' : r['usage_3G'],
                        'usage_4G_TDD' : r['usage_4G_TDD'],
                        'usage_4G_FDD' : r['usage_4G_FDD'],
                        'data_bndl_vol' : r['data_bndl_vol'],
                        'voice_vas_cnt' : r['voice_vas_cnt'],
                        'voice_vas_amnt' :r['voice_vas_amnt'],
                        'voice_vas_main_vol' : r['voice_vas_main_vol'],
                        'voice_vas_bndl_vol' : r['voice_vas_bndl_vol'],
                        'sms_vas_cnt' : r['sms_vas_cnt'],
                        'sms_vas_bndl_cnt' : r['sms_vas_bndl_cnt'],
                        'sms_vas_amnt' : r['sms_vas_amnt']  
                    }
    return data['data']