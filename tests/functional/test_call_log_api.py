def test_create_telephone_data(app):
    data = [
        {'call_id': 45063, 'time_start': '2017-09-20 15:03:59',
         'time_end': '2017-09-20 15:04:18', 'talk_time': 19, 'cpn': '213',
         'cdpn': '13823058578', 'duration': 0,
         'recording': '20170920/213_13823058578_20170920_150359_B007_cg.wav',
         'gh': '', 'xm': ''},
        {'call_id': 45065, 'time_start': '2017-09-20 15:04:25',
         'time_end': '2017-09-20 15:05:48', 'talk_time': 83, 'cpn': '213',
         'cdpn': '13825642566', 'duration': 62,
         'recording': '20170920/213_13825642566_20170920_150426_B009_cg.wav',
         'gh': '', 'xm': ''},
        {'call_id': 45067, 'time_start': '2017-09-20 15:14:14',
         'time_end': '2017-09-20 15:14:20', 'talk_time': 6, 'cpn': '213',
         'cdpn': '08119955887', 'duration': 0,
         'recording': '20170920/213_08119955887_20170920_151414_B00B_cg.wav',
         'gh': '', 'xm': ''},
    ]
    results = app.get('/api/v1/call-logs/max_call_id')
    assert results.status == '200 OK'
    assert results.json['data'] == 0

    results = app.post_json('/api/v1/call-logs', data)
    assert results.status == '200 OK'
    data = results.json['data']
    assert data['state'] == 'succeed'
    assert data['msg'] == 'Insert many succeed'

    results = app.get('/api/v1/call-logs/max_call_id')
    assert results.status == '200 OK'
    assert results.json['data'] == 45067

