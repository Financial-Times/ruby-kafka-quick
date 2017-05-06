require "poseidon"
require 'rubygems'
require 'json'
require "base64"
require 'nokogiri'
require 'digest'
require 'securerandom'

def createMsg(imageSet)
  lastModified = Time.now.strftime('%Y-%m-%dT%I:%M:%S%z')
  header = ['X-Request-Id:' + SecureRandom.uuid,
  'Message-Timestamp:' + lastModified,
  'Message-Id:' + SecureRandom.uuid,
  'Message-Type:cms-content-published',
  'Content-Type:application/json',
  'Origin-System-Id: http://cmdb.ft.com/systems/methode-web-pub',
  '',
  ''].join("\n")
  body = {
    :contentUri => "http://methode-article-image-set-mapper.svc.ft.com/image-set/model/#{imageSet[:uuid]}",
    :payload => imageSet,
    :lastModified => lastModified
  }
  marshaledBody = JSON.generate(body)
  return header + marshaledBody
end

def parseMsg(m)
  native = JSON.parse(m.lines.drop(8)[0])
  puts native['uuid']
  if native['type'] != 'EOM::CompoundStory'
    return
  end
  decoded64 = Base64.decode64(native['value'])
  doc = Nokogiri::XML(decoded64)
  imageSets = doc.xpath('//doc/story/text/body/image-set')
  messages = []
  imageSets.each do |xmlImageSet|
    members = Array.new
    jsonImageSet = { :uuid => Digest::MD5.hexdigest(xmlImageSet['id']), :members => members }
    members << { :uuid => xmlImageSet.at_xpath('//image-medium')['fileref'].split('=')[1] }
    members << { :uuid => xmlImageSet.at_xpath('//image-small')['fileref'].split('=')[1] }
    members << { :uuid => xmlImageSet.at_xpath('//image-large')['fileref'].split('=')[1] }
    messages << createMsg(jsonImageSet)
  end
  messages
end

# File.open("sample-message.txt", "r") do |m|
#   msgs = parseMsg(m)
#   msgs.each do |msg|
#     puts msg
#   end
# end

consumer = Poseidon::PartitionConsumer.new("methode-article-image-set-mapper", "ip-172-24-45-243.eu-west-1.compute.internal", 9092, "NativeCmsPublicationEvents", 0, :latest_offset)
producer = Poseidon::Producer.new(["ip-172-24-45-243.eu-west-1.compute.internal:9092"], "methode-article-image-set-mapper-producer")
loop do
  messages = consumer.fetch
  messages.each do |m|
    msgs = parseMsg(m.value.to_s)
    msgs.each do |msg|
      producer.send_messages(Poseidon::MessageToSend.new("CmsPublicationEvents", msg))
      puts msg
    end
  end
end
