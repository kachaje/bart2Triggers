#!/usr/bin/env ruby
require "cgi"
require "mysql"
require "yaml"
require "date"

cgi = CGI.new("html4")

def reply(statement)
  puts <<EOS
Content-type: text/html

#{statement}
EOS
end

def connect(env)  
  host = YAML::load_file('database.yml')[env]['host']
  user = YAML::load_file('database.yml')[env]['username']
  pass = YAML::load_file('database.yml')[env]['password']
  db = YAML::load_file('database.yml')[env]['database']

  con = Mysql.connect(host, user, pass, db)
end

def current_site
  con = connect("development")
  
  rs = con.query("SELECT property_value FROM global_property WHERE property = 'current_health_center_name' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(titleize(row["property_value"]))
end

def quarter(start_date=Time.now.strftime("%Y-%m-%d"), end_date=Time.now.strftime("%Y-%m-%d"), section=nil)
  startdate = Date.parse(start_date)
  enddate = Date.parse(end_date)
  
  retstr = ""
  
  if startdate.year == enddate.year
    if ((startdate.month - 1)/3) == ((enddate.month - 1)/3)
      q = ((startdate.month - 1)/3)
      
      case q.to_s
      when "0":
          retstr = startdate.year.to_s + " - 1<sup>st</sup> Quarter"
      when "1":
          retstr = startdate.year.to_s + " - 2<sup>nd</sup> Quarter"
      when "2":
          retstr = startdate.year.to_s + " - 3<sup>rd</sup> Quarter"
      when "3":
          retstr = startdate.year.to_s + " - 4<sup>th</sup> Quarter"
      end
    else
      retstr = startdate.strftime("%d/%b/%Y") + " to " + enddate.strftime("%d/%b/%Y")
    end
  else
    retstr = startdate.strftime("%d/%b/%Y") + " to " + enddate.strftime("%d/%b/%Y")
  end
  
	reply(retstr)  
end

def titleize(string)
  words = string.split(" ")
  output = ""
  
  words.each{|word|
    output += " " + word.strip[0,1].upcase + word[1,word.length-1].downcase.strip
  }
  
  output = output.strip
end

# Start Cohort queries

def new_total_reg(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def cum_total_reg(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      "<= '#{end_date}' AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def new_ft(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report "+ 
      " WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND DATE(registration_date) " + 
      "= DATE(art_start_date) AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def cum_ft(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report "+ 
      " WHERE DATE(registration_date) <= '#{end_date}' AND DATE(registration_date) " + 
      "= DATE(art_start_date) AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def new_re(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' " + 
      " AND DATE(art_start_date) != DATE(registration_date)" + 
      " AND COALESCE(patient_did_not_take_arvs_in_last_two_months,'0000-00-00') != '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def cum_re(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      "<= '#{end_date}' AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' " +  
      " AND DATE(art_start_date) != DATE(registration_date)" + 
      " AND COALESCE(patient_did_not_take_arvs_in_last_two_months,'0000-00-00') != '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def new_ti(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND DATE(registration_date) " + 
      "!= DATE(art_start_date) AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND " +
      "COALESCE(patient_did_not_take_arvs_in_last_two_months,'0000-00-00') = '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def cum_ti(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' " + 
      "AND DATE(registration_date) " + 
      "!= DATE(art_start_date) AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND " +
      "COALESCE(patient_did_not_take_arvs_in_last_two_months,'0000-00-00') = '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def new_males(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND gender = 'M' " + 
      "AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def cum_males(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      "<= '#{end_date}' AND gender = 'M' AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def new_non_preg(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND gender = 'F' " + 
      "AND COALESCE(patient_pregnant_date, '') = '' AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def cum_non_preg(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      "<= '#{end_date}' AND gender = 'F' " + 
      "AND COALESCE(patient_pregnant_date, '') = '' AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def new_preg_all_age(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND gender = 'F' " + 
      "AND COALESCE(patient_pregnant_date, '') != '' AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def cum_preg_all_age(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      "<= '#{end_date}' AND gender = 'F' " + 
      "AND COALESCE(patient_pregnant_date, '') != '' AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def new_a(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND age_initiation < 2 LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def cum_a(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      "<= '#{end_date}' AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND age_initiation < 2 LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def new_b(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND age_initiation >= 2 AND age_initiation <= 14 LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def cum_b(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      "<= '#{end_date}' AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND age_initiation >= 2 AND age_initiation <= 14 LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def new_c(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND age_initiation > 14 LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def cum_c(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      "<= '#{end_date}' AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND age_initiation > 14 LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def new_unk_age(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(age_initiation,0) = 0 LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def cum_unk_age(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT COUNT(*) fields FROM patient_report WHERE DATE(registration_date) " + 
      "<= '#{end_date}' AND COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(age_initiation,0) = 0 LIMIT 0,1")
  
  row = rs.fetch_hash
  
  reply(row["fields"])
end

def new_pres_hiv(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(reason_for_art_eligibility,'') = " + 
      "'PRESUMED SEVERE HIV'")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_pres_hiv(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(reason_for_art_eligibility,'') = " + 
      "'PRESUMED SEVERE HIV'")
  
  row = rs.num_rows
  
  reply(row)
end

def new_conf_hiv(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND (COALESCE(reason_for_art_eligibility,'') = " + 
      "'HIV DNA POLYMERASE CHAIN REACTION' OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'HIV PCR')")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_conf_hiv(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND (COALESCE(reason_for_art_eligibility,'') = " + 
      "'HIV DNA POLYMERASE CHAIN REACTION' OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'HIV PCR')")
  
  row = rs.num_rows
  
  reply(row)
end

def new_who_1_2(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND (COALESCE(reason_for_art_eligibility,'') = " + 
      "'CD4 COUNT LESS THAN OR EQUAL TO 350' OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'CD4 COUNT LESS THAN OR EQUAL TO 750')")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_who_1_2(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND (COALESCE(reason_for_art_eligibility,'') = " + 
      "'CD4 COUNT LESS THAN OR EQUAL TO 350' OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'CD4 COUNT LESS THAN OR EQUAL TO 750')")
  
  row = rs.num_rows
  
  reply(row)
end

def new_who_2(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(reason_for_art_eligibility,'') = " + 
      "'LYMPHOCYTE COUNT BELOW THRESHOLD WITH WHO STAGE 2'")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_who_2(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(reason_for_art_eligibility,'') = " + 
      "'LYMPHOCYTE COUNT BELOW THRESHOLD WITH WHO STAGE 2'")
  
  row = rs.num_rows
  
  reply(row)
end

def new_children(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND (COALESCE(reason_for_art_eligibility,'') = " + 
      "'HIV infected')")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_children(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND (COALESCE(reason_for_art_eligibility,'') = " + 
      "'HIV infected')")
  
  row = rs.num_rows
  
  reply(row)
end

def new_breastfeed(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND (COALESCE(reason_for_art_eligibility,'') = " + 
      "'Breastfeeding')")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_breastfeed(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND (COALESCE(reason_for_art_eligibility,'') = " + 
      "'Breastfeeding')")
  
  row = rs.num_rows
  
  reply(row)
end

def new_preg(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND (COALESCE(reason_for_art_eligibility,'') = " + 
      "'PATIENT PREGNANT')")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_preg(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND (COALESCE(reason_for_art_eligibility,'') = " + 
      "'PATIENT PREGNANT')")
  
  row = rs.num_rows
  
  reply(row)
end

def new_who_3(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(reason_for_art_eligibility,'') LIKE " + 
      "'WHO STAGE III%'")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_who_3(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(reason_for_art_eligibility,'') LIKE " + 
      "'WHO STAGE III%'")
  
  row = rs.num_rows
  
  reply(row)
end

def new_who_4(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(reason_for_art_eligibility,'') LIKE " + 
      "'WHO STAGE IV%'")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_who_4(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(reason_for_art_eligibility,'') LIKE " + 
      "'WHO STAGE IV%'")
  
  row = rs.num_rows
  
  reply(row)
end

def new_other_reason(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) " + 
      ">= '#{start_date}' AND DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND NOT (COALESCE(reason_for_art_eligibility,'') = " + 
      "'PRESUMED SEVERE HIV' OR (COALESCE(reason_for_art_eligibility,'') = " + 
      "'HIV DNA POLYMERASE CHAIN REACTION' OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'HIV infected') OR (COALESCE(reason_for_art_eligibility,'') = " + 
      "'CD4 COUNT LESS THAN OR EQUAL TO 350' OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'CD4 COUNT LESS THAN OR EQUAL TO 750') OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'LYMPHOCYTE COUNT BELOW THRESHOLD WITH WHO STAGE 2' OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'Breastfeeding' OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'PATIENT PREGNANT' OR COALESCE(reason_for_art_eligibility,'') LIKE " + 
      "'WHO STAGE III%' OR COALESCE(reason_for_art_eligibility,'') LIKE " + 
      "'WHO STAGE IV%' OR COALESCE(reason_for_art_eligibility,'') = '')")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_other_reason(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND NOT (COALESCE(reason_for_art_eligibility,'') = " + 
      "'PRESUMED SEVERE HIV' OR (COALESCE(reason_for_art_eligibility,'') = " + 
      "'HIV DNA POLYMERASE CHAIN REACTION' OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'HIV infected') OR (COALESCE(reason_for_art_eligibility,'') = " + 
      "'CD4 COUNT LESS THAN OR EQUAL TO 350' OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'CD4 COUNT LESS THAN OR EQUAL TO 750') OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'LYMPHOCYTE COUNT BELOW THRESHOLD WITH WHO STAGE 2' OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'Breastfeeding' OR COALESCE(reason_for_art_eligibility,'') = " + 
      "'PATIENT PREGNANT' OR COALESCE(reason_for_art_eligibility,'') LIKE " + 
      "'WHO STAGE III%' OR COALESCE(reason_for_art_eligibility,'') LIKE " + 
      "'WHO STAGE IV%' OR COALESCE(reason_for_art_eligibility,'') = '')")
  
  row = rs.num_rows
  
  reply(row)
end

def new_no_tb(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT d.patient_id fields FROM patient_report_details d LEFT OUTER JOIN patient_report p" + 
      " ON d.patient_id = p.patient_id WHERE DATE(p.registration_date) " + 
      ">= '#{start_date}' AND DATE(p.registration_date) <= '#{end_date}' AND " + 
      "COALESCE(p.art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(d.current_episode_of_tb,'0000-00-00') = '0000-00-00' " + 
      "AND COALESCE(d.tb_within_the_last_2_years,'0000-00-00') = '0000-00-00'")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_no_tb(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT d.patient_id fields FROM patient_report_details d LEFT OUTER JOIN patient_report p" + 
      " ON d.patient_id = p.patient_id WHERE DATE(p.registration_date) <= '#{end_date}' AND " + 
      "COALESCE(p.art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(d.current_episode_of_tb,'0000-00-00') = '0000-00-00' " + 
      "AND COALESCE(d.tb_within_the_last_2_years,'0000-00-00') = '0000-00-00'")
  
  row = rs.num_rows
  
  reply(row)
end

def new_tb_w2yrs(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT d.patient_id FROM patient_report_details d LEFT OUTER JOIN patient_report p" + 
      " ON d.patient_id = p.patient_id WHERE DATE(p.registration_date) " + 
      ">= '#{start_date}' AND DATE(p.registration_date) <= '#{end_date}' AND " + 
      "COALESCE(p.art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(d.current_episode_of_tb,'0000-00-00') = '0000-00-00' " + 
      "AND COALESCE(d.tb_within_the_last_2_years,'0000-00-00') != '0000-00-00' AND " + 
      "DATE(d.tb_within_the_last_2_years) <= '#{end_date}'")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_tb_w2yrs(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT d.patient_id FROM patient_report_details d LEFT OUTER JOIN patient_report p" + 
      " ON d.patient_id = p.patient_id WHERE DATE(p.registration_date) <= '#{end_date}' AND " + 
      "COALESCE(p.art_start_date,'0000-00-00') != '0000-00-00' AND COALESCE(d.current_episode_of_tb,'0000-00-00') = '0000-00-00' " + 
      "AND COALESCE(d.tb_within_the_last_2_years,'0000-00-00') != '0000-00-00' AND " + 
      "DATE(d.tb_within_the_last_2_years) <= '#{end_date}'")
  
  row = rs.num_rows
  
  reply(row)
end

def new_current_tb(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT d.patient_id FROM patient_report_details d LEFT OUTER JOIN patient_report p" + 
      " ON d.patient_id = p.patient_id WHERE DATE(p.registration_date) " + 
      ">= '#{start_date}' AND DATE(p.registration_date) <= '#{end_date}' AND " + 
      "COALESCE(p.art_start_date,'0000-00-00') != '0000-00-00' AND " +
      "COALESCE(d.current_episode_of_tb,'0000-00-00') != '0000-00-00' AND " + 
      "DATE(d.current_episode_of_tb) <= '#{end_date}'")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_current_tb(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT d.patient_id FROM patient_report_details d LEFT OUTER JOIN patient_report p" + 
      " ON d.patient_id = p.patient_id WHERE DATE(p.registration_date) <= '#{end_date}' AND " + 
      "COALESCE(p.art_start_date,'0000-00-00') != '0000-00-00' AND " +
      "COALESCE(d.current_episode_of_tb,'0000-00-00') != '0000-00-00' AND " + 
      "DATE(d.current_episode_of_tb) <= '#{end_date}'")
  
  row = rs.num_rows
  
  reply(row)
end

def new_ks(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT d.patient_id FROM patient_report_details d LEFT OUTER JOIN patient_report p" + 
      " ON d.patient_id = p.patient_id WHERE DATE(p.registration_date) " + 
      ">= '#{start_date}' AND DATE(p.registration_date) <= '#{end_date}' AND " + 
      "COALESCE(p.art_start_date,'0000-00-00') != '0000-00-00' AND " +
      "COALESCE(d.karposis_sarcoma,'0000-00-00') != '0000-00-00' AND " + 
      "DATE(d.karposis_sarcoma) <= '#{end_date}'")
  
  row = rs.num_rows
  
  reply(row)
end

def cum_ks(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT d.patient_id FROM patient_report_details d LEFT OUTER JOIN patient_report p" + 
      " ON d.patient_id = p.patient_id WHERE DATE(p.registration_date) <= '#{end_date}' AND " + 
      "COALESCE(p.art_start_date,'0000-00-00') != '0000-00-00' AND " +
      "COALESCE(d.karposis_sarcoma,'0000-00-00') != '0000-00-00' AND " + 
      "DATE(d.karposis_sarcoma) <= '#{end_date}'")
  
  row = rs.num_rows
  
  reply(row)
end

def total_on_art(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}')")
  
  row = rs.num_rows
  
  reply(row)
end

def died_1st_month(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p " + 
      "WHERE DATE(p.registration_date) <= '#{end_date}' AND " + 
      "COALESCE(p.art_start_date,'0000-00-00') != '0000-00-00' AND (CASE WHEN COALESCE(p.art_start_date,'0000-00-00') != '0000-00-00' " + 
      "AND COALESCE(p.latest_state_date,'') != '' THEN DATEDIFF(p.latest_state_date, p.art_start_date) " + 
      "ELSE 31 END) <= 30 AND p.latest_state = 'Patient died'")
  
  row = rs.num_rows
  
  reply(row)
end

def died_2nd_month(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND (CASE WHEN COALESCE(art_start_date,'0000-00-00') != '0000-00-00' " + 
      "AND COALESCE(latest_state_date,'') != '' THEN DATEDIFF(latest_state_date, art_start_date) " + 
      "ELSE 31 END) > 30 AND (CASE WHEN COALESCE(art_start_date,'0000-00-00') != '0000-00-00' " + 
      "AND COALESCE(latest_state_date,'') != '' THEN DATEDIFF(latest_state_date, art_start_date) " + 
      "ELSE 61 END) <= 60 AND latest_state = 'Patient died'")
  
  row = rs.num_rows
  
  reply(row)
end

def died_3rd_month(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND (CASE WHEN COALESCE(art_start_date,'0000-00-00') != '0000-00-00' " + 
      "AND COALESCE(latest_state_date,'') != '' THEN DATEDIFF(latest_state_date, art_start_date) " + 
      "ELSE 31 END) > 60 AND (CASE WHEN COALESCE(art_start_date,'0000-00-00') != '0000-00-00' " + 
      "AND COALESCE(latest_state_date,'') != '' THEN DATEDIFF(latest_state_date, art_start_date) " + 
      "ELSE 91 END) <= 90 AND latest_state = 'Patient died'")
  
  row = rs.num_rows
  
  reply(row)
end

def died_after_3rd_month(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND (CASE WHEN COALESCE(art_start_date,'0000-00-00') != '0000-00-00' " + 
      "AND COALESCE(latest_state_date,'') != '' THEN DATEDIFF(latest_state_date, art_start_date) " + 
      "ELSE 0 END) > 90 AND latest_state = 'Patient died'")
  
  row = rs.num_rows
  
  reply(row)
end

def died_total(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT patient_id FROM patient_report WHERE DATE(registration_date) <= '#{end_date}' AND " + 
      "COALESCE(art_start_date,'0000-00-00') != '0000-00-00' AND latest_state = 'Patient died' " + 
      "AND DATE(latest_state_date) < '#{end_date}'")
  
  row = rs.num_rows
  
  reply(row)
end

def defaulted(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT d.patient_id FROM patient_report_details d LEFT OUTER JOIN patient_report p " + 
      " ON d.patient_id = p.patient_id WHERE DATE(p.registration_date) <= '#{end_date}' AND " + 
      "COALESCE(p.art_start_date,'0000-00-00') != '0000-00-00' AND (CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}' AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}')")
  
  row = rs.num_rows
  
  reply(row)
end

def stopped(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT d.patient_id FROM patient_report_details d LEFT OUTER JOIN patient_report p " + 
      " ON d.patient_id = p.patient_id WHERE DATE(p.registration_date) <= '#{end_date}' AND " + 
      "COALESCE(p.art_start_date,'0000-00-00') != '0000-00-00' AND (CASE WHEN COALESCE(d.arv_drugs_stopped,'') != '' " + 
      "THEN DATE(d.arv_drugs_stopped) <= '#{end_date}' " + 
      "ELSE 0 END)")
  
  row = rs.num_rows
  
  reply(row)
end

def transfered(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT d.patient_id FROM patient_report_details d LEFT OUTER JOIN patient_report p " + 
      " ON d.patient_id = p.patient_id WHERE DATE(p.registration_date) <= '#{end_date}' AND " + 
      "COALESCE(p.art_start_date,'0000-00-00') != '0000-00-00' AND DATE(d.latest_state_date) <= '#{end_date}' " + 
      "AND d.latest_state = 'Patient transferred out'")
  
  row = rs.num_rows
  
  reply(row)
end

def unknown_outcome(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN patient_report_details d ON p.patient_id = " + 
      "d.patient_id WHERE COALESCE(p.art_start_date, '0000-00-00') != '0000-00-00' AND " + 
      "COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' AND DATE(p.registration_date) " + 
      "<= '#{end_date}' AND not((d.latest_state = 'Patient died' AND d.latest_state_date <= '#{end_date}') " + 
      "OR (d.latest_state = 'Patient transferred out' AND d.latest_state_date <= '#{end_date}') OR " + 
      " (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' AND d.arv_drugs_stopped <= '#{end_date}') " + 
      "OR (datediff(d.last_visit_date, d.expiry_date_for_last_arvs) >= 60 AND DATE(d.expiry_date_for_last_arvs) " + 
      "< '#{end_date}') OR (d.latest_state = 'On antiretrovirals' AND d.latest_state_date <= '#{end_date}') " + 
      "OR (d.latest_state = 'Treatment stopped' AND d.latest_state_date <= '#{end_date}'))")
  
  row = rs.num_rows
  
  reply(row)
end

def n1a(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND d.latest_regimen = '1A'")
  
  row = rs.num_rows
  
  reply(row)
end

def n1p(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND d.latest_regimen = '1P'")
  
  row = rs.num_rows
  
  reply(row)
end

def n2a(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND d.latest_regimen = '2A'")
  
  row = rs.num_rows
  
  reply(row)
end

def n2p(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND d.latest_regimen = '2P'")
  
  row = rs.num_rows
  
  reply(row)
end

def n3a(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND d.latest_regimen = '3A'")
  
  row = rs.num_rows
  
  reply(row)
end

def n3p(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND d.latest_regimen = '3P'")
  
  row = rs.num_rows
  
  reply(row)
end

def n4a(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND d.latest_regimen = '4A'")
  
  row = rs.num_rows
  
  reply(row)
end

def n4p(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND d.latest_regimen = '4P'")
  
  row = rs.num_rows
  
  reply(row)
end

def n5a(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND d.latest_regimen = '5A'")
  
  row = rs.num_rows
  
  reply(row)
end

def n6a(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND d.latest_regimen = '6A'")
  
  row = rs.num_rows
  
  reply(row)
end

def n7a(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND d.latest_regimen = '7A'")
  
  row = rs.num_rows
  
  reply(row)
end

def n8a(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND d.latest_regimen = '8A'")
  
  row = rs.num_rows
  
  reply(row)
end

def n9p(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND d.latest_regimen = '9P'")
  
  row = rs.num_rows
  
  reply(row)
end

def non_std(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND (COALESCE(d.latest_regimen,'Nn') = 'Nn')")
  
  row = rs.num_rows
  
  reply(row)
end

def tb_no_suspect(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND DATE(d.tb_status_date) <= '#{end_date}' " + 
      "AND (d.tb_status = 'TB NOT suspected' OR d.tb_status = 'Nosup')")
  
  row = rs.num_rows
  
  reply(row)
end

def tb_suspected(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND DATE(d.tb_status_date) <= '#{end_date}' " + 
      "AND (d.tb_status = 'TB suspected' OR d.tb_status = 'sup')")
  
  row = rs.num_rows
  
  reply(row)
end

def tb_confirm_not_treat(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND DATE(d.tb_status_date) <= '#{end_date}' " + 
      "AND (d.tb_status = 'Confirmed TB not on treatment' OR d.tb_status = 'Norx')")
  
  row = rs.num_rows
  
  reply(row)
end

def tb_confirmed(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND DATE(d.tb_status_date) <= '#{end_date}' " + 
      "AND (d.tb_status = 'Confirmed TB on treatment' OR d.tb_status = 'RX')")
  
  row = rs.num_rows
  
  reply(row)
end

def unknown_tb(start_date=Time.now, end_date=Time.now, section=nil)
	con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND COALESCE(d.tb_status,'Unknown') = 'Unknown'" + 
      " AND DATE(d.tb_status_date) <= '#{end_date}'")
  
  row = rs.num_rows
  
  reply(row)
end

def side_effects(start_date=Time.now, end_date=Time.now, section=nil)
  con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND COALESCE(d.latest_side_effects, '0000-00-00') != '0000-00-00' AND " + 
      "d.latest_side_effects <= '#{end_date}'")
  
  row = rs.num_rows
  
  reply(row)
end

def missed_0_6(start_date=Time.now, end_date=Time.now, section=nil)
  con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND COALESCE(d.missed_drugs_count, 0) <= 6 AND " + 
      "COALESCE(d.last_missed_drugs_date,'0000-00-00') <= '#{end_date}'")
  
  row = rs.num_rows
  
  reply(row)
end

def missed_7plus(start_date=Time.now, end_date=Time.now, section=nil)
  con = connect("development")
  
  rs = con.query("SELECT DISTINCT p.patient_id FROM patient_report p LEFT OUTER JOIN " + 
      "patient_report_details d ON p.patient_id = d.patient_id WHERE COALESCE(p.art_start_date, " + 
      "'0000-00-00') != '0000-00-00' AND COALESCE(p.registration_date, '0000-00-00') != '0000-00-00' " + 
      "AND DATE(p.registration_date) <= '#{end_date}' AND not((d.latest_state = 'Patient died' " + 
      "AND d.latest_state_date <= '#{end_date}') OR (d.latest_state = 'Patient transferred out' AND " + 
      "d.latest_state_date <= '#{end_date}') OR (COALESCE(d.arv_drugs_stopped,'0000-00-00') != '0000-00-00' " + 
      "AND d.arv_drugs_stopped <= '#{end_date}') OR ((CASE WHEN COALESCE(p.expiry_date_for_last_arvs,'') != '' " + 
      "AND COALESCE(d.last_visit_date,'0000-00-00') != '0000-00-00' THEN DATEDIFF(d.last_visit_date, p.expiry_date_for_last_arvs) " + 
      "ELSE 0 END) >= 60 AND DATE(p.expiry_date_for_last_arvs) <= '#{end_date}')) AND (d.latest_state = 'On antiretrovirals' AND " + 
      "d.latest_state_date <= '#{end_date}') AND COALESCE(d.missed_drugs_count, 0) >= 7 AND " + 
      "COALESCE(d.last_missed_drugs_date,'0000-00-00') <= '#{end_date}'")
  
  row = rs.num_rows
  
  reply(row)
end

# End cohort queries

if cgi.params["field"]
  
  if cgi.params["start_date"]
    start_date = cgi.params["start_date"][0]
  else 
    start_date = Time.now.strftime("%Y-%m-%d")
  end
  if cgi.params["end_date"]
    end_date = cgi.params["end_date"][0]
  else 
    end_date = Time.now.strftime("%Y-%m-%d")
  end
  
  case cgi.params["field"][0]
  when "new_total_reg"
    new_total_reg(start_date, end_date, cgi.params["field"][0])
  when "cum_total_reg"
    cum_total_reg(start_date, end_date, cgi.params["field"][0])
  when "new_ft"
    new_ft(start_date, end_date, cgi.params["field"][0])
  when "cum_ft"
    cum_ft(start_date, end_date, cgi.params["field"][0])
  when "new_re"
    new_re(start_date, end_date, cgi.params["field"][0])
  when "cum_re"
    cum_re(start_date, end_date, cgi.params["field"][0])
  when "new_ti"
    new_ti(start_date, end_date, cgi.params["field"][0])
  when "cum_ti"
    cum_ti(start_date, end_date, cgi.params["field"][0])
  when "new_males"
    new_males(start_date, end_date, cgi.params["field"][0])
  when "cum_males"
    cum_males(start_date, end_date, cgi.params["field"][0])
  when "new_non_preg"
    new_non_preg(start_date, end_date, cgi.params["field"][0])
  when "cum_non_preg"
    cum_non_preg(start_date, end_date, cgi.params["field"][0])
  when "new_preg_all_age"
    new_preg_all_age(start_date, end_date, cgi.params["field"][0])
  when "cum_preg_all_age"
    cum_preg_all_age(start_date, end_date, cgi.params["field"][0])
  when "new_a"
    new_a(start_date, end_date, cgi.params["field"][0])
  when "cum_a"
    cum_a(start_date, end_date, cgi.params["field"][0])
  when "new_b"
    new_b(start_date, end_date, cgi.params["field"][0])
  when "cum_b"
    cum_b(start_date, end_date, cgi.params["field"][0])
  when "new_c"
    new_c(start_date, end_date, cgi.params["field"][0])
  when "cum_c"
    cum_c(start_date, end_date, cgi.params["field"][0])
  when "new_unk_age"
    new_unk_age(start_date, end_date, cgi.params["field"][0])
  when "cum_unk_age"
    cum_unk_age(start_date, end_date, cgi.params["field"][0])
  when "new_pres_hiv"
    new_pres_hiv(start_date, end_date, cgi.params["field"][0])
  when "cum_pres_hiv"
    cum_pres_hiv(start_date, end_date, cgi.params["field"][0])
  when "new_conf_hiv"
    new_conf_hiv(start_date, end_date, cgi.params["field"][0])
  when "cum_conf_hiv"
    cum_conf_hiv(start_date, end_date, cgi.params["field"][0])
  when "new_who_1_2"
    new_who_1_2(start_date, end_date, cgi.params["field"][0])
  when "cum_who_1_2"
    cum_who_1_2(start_date, end_date, cgi.params["field"][0])
  when "new_who_2"
    new_who_2(start_date, end_date, cgi.params["field"][0])
  when "cum_who_2"
    cum_who_2(start_date, end_date, cgi.params["field"][0])
  when "new_children"
    new_children(start_date, end_date, cgi.params["field"][0])
  when "cum_children"
    cum_children(start_date, end_date, cgi.params["field"][0])
  when "new_breastfeed"
    new_breastfeed(start_date, end_date, cgi.params["field"][0])
  when "cum_breastfeed"
    cum_breastfeed(start_date, end_date, cgi.params["field"][0])
  when "new_preg"
    new_preg(start_date, end_date, cgi.params["field"][0])
  when "cum_preg"
    cum_preg(start_date, end_date, cgi.params["field"][0])
  when "new_who_3"
    new_who_3(start_date, end_date, cgi.params["field"][0])
  when "cum_who_3"
    cum_who_3(start_date, end_date, cgi.params["field"][0])
  when "new_who_4"
    new_who_4(start_date, end_date, cgi.params["field"][0])
  when "cum_who_4"
    cum_who_4(start_date, end_date, cgi.params["field"][0])
  when "new_other_reason"
    new_other_reason(start_date, end_date, cgi.params["field"][0])
  when "cum_other_reason"
    cum_other_reason(start_date, end_date, cgi.params["field"][0])
  when "new_no_tb"
    new_no_tb(start_date, end_date, cgi.params["field"][0])
  when "cum_no_tb"
    cum_no_tb(start_date, end_date, cgi.params["field"][0])
  when "new_tb_w2yrs"
    new_tb_w2yrs(start_date, end_date, cgi.params["field"][0])
  when "cum_tb_w2yrs"
    cum_tb_w2yrs(start_date, end_date, cgi.params["field"][0])
  when "new_current_tb"
    new_current_tb(start_date, end_date, cgi.params["field"][0])
  when "cum_current_tb"
    cum_current_tb(start_date, end_date, cgi.params["field"][0])
  when "new_ks"
    new_ks(start_date, end_date, cgi.params["field"][0])
  when "cum_ks"
    cum_ks(start_date, end_date, cgi.params["field"][0])
  when "total_on_art"
    total_on_art(start_date, end_date, cgi.params["field"][0])
  when "died_1st_month"
    died_1st_month(start_date, end_date, cgi.params["field"][0])
  when "died_2nd_month"
    died_2nd_month(start_date, end_date, cgi.params["field"][0])
  when "died_3rd_month"
    died_3rd_month(start_date, end_date, cgi.params["field"][0])
  when "died_after_3rd_month"
    died_after_3rd_month(start_date, end_date, cgi.params["field"][0])
  when "died_total"
    died_total(start_date, end_date, cgi.params["field"][0])
  when "defaulted"
    defaulted(start_date, end_date, cgi.params["field"][0])
  when "stopped"
    stopped(start_date, end_date, cgi.params["field"][0])
  when "transfered"
    transfered(start_date, end_date, cgi.params["field"][0])
  when "unknown_outcome"
    unknown_outcome(start_date, end_date, cgi.params["field"][0])
  when "n1a"
    n1a(start_date, end_date, cgi.params["field"][0])
  when "n1p"
    n1p(start_date, end_date, cgi.params["field"][0])
  when "n2a"
    n2a(start_date, end_date, cgi.params["field"][0])
  when "n2p"
    n2p(start_date, end_date, cgi.params["field"][0])
  when "n3a"
    n3a(start_date, end_date, cgi.params["field"][0])
  when "n3p"
    n3p(start_date, end_date, cgi.params["field"][0])
  when "n4a"
    n4a(start_date, end_date, cgi.params["field"][0])
  when "n4p"
    n4p(start_date, end_date, cgi.params["field"][0])
  when "n5a"
    n5a(start_date, end_date, cgi.params["field"][0])
  when "n6a"
    n6a(start_date, end_date, cgi.params["field"][0])
  when "n7a"
    n7a(start_date, end_date, cgi.params["field"][0])
  when "n8a"
    n8a(start_date, end_date, cgi.params["field"][0])
  when "n9p"
    n9p(start_date, end_date, cgi.params["field"][0])
  when "non_std"
    non_std(start_date, end_date, cgi.params["field"][0])
  when "tb_no_suspect"
    tb_no_suspect(start_date, end_date, cgi.params["field"][0])
  when "tb_suspected"
    tb_suspected(start_date, end_date, cgi.params["field"][0])
  when "tb_confirm_not_treat"
    tb_confirm_not_treat(start_date, end_date, cgi.params["field"][0])
  when "tb_confirmed"
    tb_confirmed(start_date, end_date, cgi.params["field"][0])
  when "unknown_tb"
    unknown_tb(start_date, end_date, cgi.params["field"][0])
  when "current_site"
    current_site
  when "quarter"
    quarter(start_date, end_date, cgi.params["field"][0])
  when "side_effects"
    side_effects(start_date, end_date, cgi.params["field"][0])
  when "missed_0_6"
    missed_0_6(start_date, end_date, cgi.params["field"][0])
  when "missed_7plus"
    missed_7plus(start_date, end_date, cgi.params["field"][0])
  else
    reply(cgi.params["field"][0])
  end
end
