#!/usr/bin/ruby

require 'optparse'
require 'tmpdir'
require 'net/smtp'


FROM_EMAIL = "FROM GMAIL ADDRESS HERE"
FROM_PASSWORD = "FROM GMAIL PASSWORD HERE"

S3CMD = "s3cmd"
HADOOP = "/home/hadoop"

# Send a hive report via email.
#
# This script will download files from an s3 url, concatenate them together, zip up the results
# and send it as an attachment to a specified email address.
#
# This script assumes that you are using gmail (can be a custom google accounts domain) to send
# mail.  You should modify FROM_EMAIL and FROM_PASSWORD above with the proper credentials.
#
# This script does not currently handle:
#   1. multiple email addresses (you probably want a mailing list anyway)
#   2. multiple reports per email (just run the script multiple times)
#   3. nested directories (don't partition your report)
#   4. compressed files (don't think we're using this anyway for reports)
# 
# The intended usage is to run this as a job step with your hive script, passing it in the location
# of the report results in s3.
#
# E.g.
#
# elastic-mapreduce --create --name "my awesome report ${MONTH}" \
#   --num-instances 10 --instance-type c1.medium  --hadoop-version 0.20 \
#   --hive-script --arg s3://path/to/hive/script.sql \
#   --args -d,MONTH=${MONTH} --args -d,START=${START} --args -d,END=${END} \
#   --jar s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar \
#   --args s3://path/to/emr-mailer/send-report.rb \
#   --args -n,report_${MONTH} --args -s,"my awesome report ${MONTH}" \
#   --args -e,awesome-reports@company.com \
#   --args -r,s3://path/to/report/results
#

options = {}
optparse = OptionParser.new do |opts|
  opts.banner = "Usage: #{$0} -r,--report <s3 url> -e,--email <email address>"
  
  opts.on('-n', '--name name', 'report file name') { |val|
    options[:name] = val
  }
  
  opts.on('-r', '--report s3Path', 's3 url') { |val|
    options[:report] = val
  }
  
  opts.on('-e', '--email address', 'email address') { |val|
    options[:email] = val
  }
  
  opts.on('-s', '--subject subject', 'email subject') {|val|
    options[:subject] = val
  }
  
  opts.on('-h', '--help', 'Display help') do
    puts opts
    exit
  end
end

optparse.parse!

unless (options[:report] && options[:email] && options[:name] && options[:subject])
  puts optparse
  exit 1
end



# Pull out aws credentials from hadoop config.
# hadoop-site.xml on hadoop 0.18 machines, core-site.xml on hadoop 0.20
def getCredentials()
  path = "#{HADOOP}/conf/"
  
  key = nil
  secret = nil
  
  ["core-site.xml", "hadoop-site.xml"].each do { |f|
    if File.exists?(path+f) 
      IO.readlines(path + f).each{ |line|
        if (line =~ /<property><name>fs.s3n.awsSecretAccessKey<\/name><value>(.+)<\/value><\/property>/) 
          secret = $1
        end
        if (line =~/<property><name>fs.s3n.awsAccessKeyId<\/name><value>(.+)<\/value><\/property>/) 
          key = $1
        end
      
        if (secret && key) 
          return Struct.new(:key, :secret).new(key, secret)
        end
      }
    end
  }
  
  return nil
end

# store creds and other config where s3cmd wants it
def configureS3Cmd(creds)
  path = "#{HADOOP}/.s3cfg"
  if (! File.exists?(path)) 
    File::open(path, "w") do |f|
      f << <<-S3_CONFIG
[default]
access_key = #{creds.key}
acl_public = False
bucket_location = US
encrypt = False
secret_key = #{creds.secret}
S3_CONFIG
    end
  end
end

# download report, perform concatenation, and zip.
def prepareReport(name, s3path)
  tmp = Dir.mktmpdir("report", "/mnt")
  Dir.mkdir(tmp + "/parts")
  
  puts "TMP dir is #{tmp}\n"
  
  filecount = 0
  
  if (! s3path.match(/^.*\/$/))
    s3path = s3path + "/"
  end
  
  puts "#{s3path}"
  
  `#{S3CMD} ls #{s3path}`.each { |obj|
    if (obj=~/^.*(s3:\/\/.*?([^\/]+))\n$/)
      part = $1
      file = $2
      puts "Downloading #{part} #{file}"
      system("#{S3CMD} get #{part} #{tmp}/parts/#{file}") or raise("can't download: #{part}")

      filecount+=1
    end
  }
  
  raise("no files found in #{s3path}") unless (filecount > 0)
  
  ## concat files
  Dir.mkdir(tmp + "/" + name)
  system("find #{tmp}/parts -type f | xargs cat > #{tmp}/#{name}/#{name}.csv") or raise("error creating #{name}.csv")
  
  system("zip -r -j #{tmp}/#{name}.zip #{tmp}/#{name}") or raise("error zipping #{tmp}/#{name}.zip")

  return "#{tmp}/#{name}.zip"
end

def getDomainFromAddress(address)
  if (address=~/^[^@]+@([^@]+)$/)
    return $1
  end

  raise "Can't figure out domain from #{address}. Is it a valid address?"
end

def mailReport(report, name, subject, email)
  encoded =  [File.read(report)].pack("m")
  
  marker = "1234_REPORT_MARKER"
  
  header = <<EOF
From: Automated <#{FROM_EMAIL})
To: #{email}
Subject: #{subject}
MIME-Version: 1.0
Content-Type: multipart/mixed; boundary=#{marker}
--#{marker}
EOF

  msg = <<EOF
Content-Type: text/plain
Content-Transfer-Encoding: 8bit

#{name} attached.
--#{marker}
EOF

  attach = <<EOF
Content-Type: multipart/mixed; name=\"#{name}.zip\"
Content-Transfer-Encoding:base64
Content-Disposition: attachment; filename="#{name}.zip"

#{encoded}
--#{marker}--
EOF


  mailtext = header + msg + attach

  smtp = Net::SMTP.new 'smtp.gmail.com', 587
  smtp.enable_starttls
  smtp.start(getDomainFromAddress(FROM_EMAIL), FROM_EMAIL, FROM_PASSWORD, :login)
  smtp.send_message(mailtext, FROM_EMAIL, email)
end


creds = getCredentials

configureS3Cmd(creds)
report = prepareReport(options[:name], options[:report])

puts "have report: #{report}"

mailReport(report, options[:name], options[:subject], options[:email])
