SET SESSION group_concat_max_len = 10240;

select
a.user_id 'Account number',
a.id 'LID',
a.app 'App',
a.user_name 'Customer name',
a.user_mobile_no 'applicant phone number',
a.overdue_days 'DPD', a.id_ektp 'National ID number',
a.gender 'Gender',
case a.job_industry
  when '0' then 'Finance/Bank/Insurance'
  when '1' then 'Automotive/Motorcycle'
  when '2' then 'Construction'
  when '3' then 'Uber/Grab/Gojek/Cab Driver'
  when '4' then 'Restaurant/Mall/Hotel'
  when '5' then 'Security/Cleaner'
  when '6' then 'Home Service'
  when '7' then 'Logistics/Express'
  when '8' then 'TNI/POLRI(Soldier and Police)'
  when '9' then 'Government'
  when '10' then 'Doctor/Lawyer/Accountant'
  when '11' then 'Factory'
  when '12' then 'Health/Education'
  when '13' then 'Mining/Energy'
  when '14' then 'Transportion'
  when '15' then 'Information Technology'
  when '16' then 'Telecommunication'
  else ''
end 'Customer occupation',
a.profile_city 'Home City',
REPLACE(a.profile_address, '"', '') 'Home address',
REPLACE(a.job_name, '"', '') 'Company name',
REPLACE(a.job_address, '"', '') 'Office address',
a.job_tel 'Office phone number',
a.birth_date 'Date of Birth',
a.amount 'Principle outstanding',
a.late_fee 'Fee + Penalty',
a.repaid,
a.unpaid,
a.repay_at 'Last payment date',
(
  select SUBSTRING_INDEX(GROUP_CONCAT(
    cc.name, '|',
    REPLACE(cc.number, '"', ''), '|',
    case cc.relationship
      when 0 then 'APPLICANT'
      when 1 then 'FAMILY'
      when 2 then 'COMPANY'
      when 3 then 'FC'
      else 'UNKNOWN'
    end,
    case cc.sub_relation
      when 2 then '|EC' else '|'
    end
    SEPARATOR '^'
  ), '^', 8)
  from contact cc
  where cc.user_id=a.user_id
  order by cc.relationship, cc.total_count, cc.total_duration
) 'Contacts'
from application a
where a.cycle=5 and a.status!=2 and a.overdue_days < 129
order by a.overdue_days desc;