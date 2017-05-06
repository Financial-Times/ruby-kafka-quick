require "poseidon"
require 'rubygems'
require 'json'
require "base64"
require 'nokogiri'
require 'digest'

# consumer = Poseidon::PartitionConsumer.new("methode-article-image-set-mapper", "ip-172-24-45-243.eu-west-1.compute.internal", 9092, "NativeCmsPublicationEvents", 0, :latest_offset)

# loop do
#   messages = consumer.fetch
#   messages.each do |m|
#     puts m.value
#   end
# end

File.open("sample-message.txt", "r") do |m|
  native = JSON.parse(m.lines.drop(8)[0])
  if native['type'] != 'EOM::CompoundStory'
    return
  end
  decoded64 = Base64.decode64(native['value'])
  doc = Nokogiri::XML(decoded64)
  imageSets = doc.xpath('//doc/story/text/body/image-set')
  imageSets.each do |xmlImageSet|
    members = Array.new
    jsonImageSet = { :uuid => Digest::MD5.hexdigest(xmlImageSet['id']), :members => members }
    members << { :uuid => xmlImageSet.at_xpath('//image-medium')['fileref'].split('=')[1] }
    members << { :uuid => xmlImageSet.at_xpath('//image-small')['fileref'].split('=')[1] }
    members << { :uuid => xmlImageSet.at_xpath('//image-large')['fileref'].split('=')[1] }
    puts JSON.generate(jsonImageSet)
  end
end
