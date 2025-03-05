from qryapsen import AspenConn
aspen_server = 'ARGPCS19'
aspen_conn = AspenConn(aspen_server, '')

qrytype = 'start_end'

start_datetime = '2023-05-26 00:00:00'
end_datetime = '2023-05-27 00:00:00'
date_limit = end_datetime
period = '0:30'
pairtag = 'REF1_UNIT16_SN'
tag_list = [pairtag, 'F41311_PV', 'DE41373_PV', 'C41322_PV' ]


if qrytype == 'current':
    aspen_tag_data = aspen_conn.current(tag_list)
elif qrytype == 'start_end':
    aspen_tag_data = aspen_conn.start_end(tag_list, start_datetime, end_datetime, request=5)
elif qrytype == 'aggregates':
    aspen_tag_data = aspen_conn.aggregates(tag_list, start_datetime, period)
elif qrytype == 'after':
    aspen_tag_data = aspen_conn.after(tag_list, date_limit, request=5)
else:
    aspen_tag_data = aspen_conn.start_end(tag_list, start_datetime, end_datetime, request=5)

aspen_tag_data = aspen_tag_data.sort_values(by=['TS'])
aspen_tag_data[[pairtag]] = aspen_tag_data[[pairtag]].fillna(method='ffill')
aspen_tag_data.sort_values(by=['TS']).to_clipboard()
