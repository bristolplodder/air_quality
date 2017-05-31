require  "rubygems"
require "json"
require 'open-uri'
require 'soda'
require 'nokogiri'
require 'csv'
require 'pp'


# set up your Socrata SODA credentials


client = SODA::Client.new({:domain => "XXXX", :username => "XXXX", :password => "XXXX", :app_token => "XXXX"})


time = Time.new
puts time
dst_adjust =  time.to_s[22].to_i
hour_with_zero = time.hour
dow = time.wday

if (hour_with_zero <10)
  hour_with_zero = '0'+hour_with_zero.to_s
end
hour_with_zero = hour_with_zero.to_s

puts hour_with_zero

day_with_zero = time.day
if (day_with_zero <10)
  day_with_zero = '0'+day_with_zero.to_s
end
day_with_zero = day_with_zero.to_s

month_with_zero = time.month
if (month_with_zero <10)
  month_with_zero = '0'+month_with_zero.to_s
end
month_with_zero = month_with_zero.to_s


f_n = "/home/ftp/files/"+day_with_zero+"_"+month_with_zero+"_"+time.year.to_s+" "+hour_with_zero+"_15.lsi"

locs = File.read('/home/ftp/aq_upload/input.json')
locs = JSON.parse(locs)


json = File.read(f_n)
#puts json

csv = CSV.new(json)
arr =  csv.to_a

@co = 0
@cc = 0
@links = []
@max = []
arr.each do |x|

if @co != x[0].to_i
 puts "first"
  @cc = 0
end

 
    locs.each do |z|


    if(x[0].to_i == z[0])
     @ll_id = z[0].to_i
     @ll_lat = z[2]
     @ll_long = z[3]
     @ll_name = z[4]
     @x_date =  x[1][0..9]
     @x_time = x[1][11..19]
     if(x[9].to_i == 1)
     @x_nox = x[8].to_i
     end
     if(x[11].to_i == 1)
     @x_no = x[10].to_i
     end
      if(x[13].to_i == 1)
      @x_no2 = x[12].to_i
     end

      @x_yyyy = x[1][6..9].to_i
      @x_mm = x[1][3..4].to_i
      @x_dd = x[1][0..1].to_i
      @x_hh = x[1][11..12].to_i
      @x_min = x[1][14..15].to_i


      if ((@x_time[0..1].to_i >= (hour_with_zero.to_i-dst_adjust-2) ))
        @links << [@ll_id, @ll_name, @x_date, @x_time, @x_nox, @x_no, @x_no2,@ll_lat, @ll_long,@x_yyyy, @x_mm, @x_dd, @x_hh, @x_min, dow ]
      end
    if @cc >=4
    puts "!!!!!!"
    puts @links.length
    @links.delete_at(@links.length-5)
    end

    end

  end


@cc = @cc + 1


@co = x[0].to_i

end



# create hourly mean
@avg =[]
for i in 1..12
aa = 0
ct = 0
@links.each do |a|

if (i == a[0]) 
if ((@x_time[0..1].to_i >= (hour_with_zero.to_i-dst_adjust-1) ))
aa += a[6] 
ct += 1
avg_no2 = aa/ct
if (ct == 4)
@avg << [a[0],a[1],a[2],a[3],a[4],a[5],a[6],a[7],a[8],avg_no2]
end
end
end
end
end

  update = []
 @links.each do |x|

# create rows in format that can be uploadd by SODA

     update << {
    "monitor_id" => x[0],
    "monitor_description" => x[1],
    "date" => x[2],
    "time" => x[3],
    "nox" => x[4],
    "no" => x[5],
    "no2" => x[6],
    "lat" => x[7],
    "long" => x[8],
    "year" => x[9],
    "month" => x[10],
    "day" => x[11],
    "hour" => x[12],
    "minute" => x[13],
    "day_of_week" => x[14],
    "location" => {
    "latitude" => x[7],
    "longitude" => x[8]
    }
}



end


 avg_out = []
 @avg.each do |x|

# create rows in format that can be uploadd by SODA

     avg_out << {
    "monitor_id" => x[0],
    "monitor_description" => x[1],
    "date" => x[2],
    "time" => x[3],
    "nox" => x[4],
    "no" => x[5],
    "no2" => x[6],
    "hrly_avg_no2" => x[9],
    "lat" => x[7],
    "long" => x[8],
    "location" => {
    "latitude" => x[7],
    "longitude" => x[8]
    }
}



end

puts 'snapshot'
puts avg_out
puts 'historic'
puts update

#amend dataset IDs to match yours

# put creates the "snapshot" of latest condition

  @response = client.put("XXXX-XXXX", avg_out)


# post updates the historic file with latest conditions appended
  @response = client.post("XXXX-XXXX", update)
