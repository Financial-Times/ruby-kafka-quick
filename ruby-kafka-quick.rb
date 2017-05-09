require "poseidon"
require 'rubygems'
require 'json'
require "base64"
require 'nokogiri'
require 'digest'
require 'securerandom'
require 'uuidtools'

def createMsg(imageSet)
  lastModified = Time.now.utc.iso8601(3)
  header = ['FTMSG/1.0',
  'Message-Id: ' + SecureRandom.uuid,
  'Message-Timestamp: ' + lastModified,
  'Message-Type: cms-content-published',
  'Origin-System-Id: http://cmdb.ft.com/systems/methode-web-pub',
  'Content-Type: application/json',
  'X-Request-Id: ' + SecureRandom.uuid,
  '',
  ''].join("\r\n")
  body = {
    :contentUri => "http://methode-article-image-set-mapper.svc.ft.com/image-set/model/#{imageSet[:uuid]}",
    :payload => imageSet,
    :lastModified => lastModified
  }
  return header + JSON.generate(body)
end

def parseMsg(m)
  native = JSON.parse(m.lines.drop(8)[0])
  if native['type'] != 'EOM::CompoundStory'
    puts "skipping #{native['uuid']}"
    return []
  end
  decoded64 = Base64.decode64(native['value'])
  doc = Nokogiri::XML(decoded64)
  imageSets = doc.xpath('//doc/story/text/body/image-set')
  messages = []
  imageSets.each do |xmlImageSet|
    members = Array.new
    jsonImageSet = { :uuid => UUIDTools::UUID.sha1_create(UUIDTools::UUID_DNS_NAMESPACE, xmlImageSet['id']).to_s, :members => members }
    members << { :uuid => xmlImageSet.at_xpath('//image-medium')['fileref'].split('=')[1] }
    members << { :uuid => xmlImageSet.at_xpath('//image-small')['fileref'].split('=')[1] }
    members << { :uuid => xmlImageSet.at_xpath('//image-large')['fileref'].split('=')[1] }
    messages << createMsg(jsonImageSet)
  end
  return messages
end

# File.open("sample-message.txt", "r") do |m|
#   parseMsg(m).each { |msg| puts msg }
# end

consumer = Poseidon::PartitionConsumer.new("methode-article-image-set-mapper", "ip-172-24-11-64.eu-west-1.compute.internal", 9092, "NativeCmsPublicationEvents", 0, :latest_offset)
producer = Poseidon::Producer.new(["ip-172-24-11-64.eu-west-1.compute.internal:9092"], "methode-article-image-set-mapper-producer")
loop do
  messages = consumer.fetch
  messages.each do |m|
    msgs = parseMsg(m.value.to_s)
    msgs.each { |msg| puts msg }
    producer.send_messages(msgs.map { |msg| Poseidon::MessageToSend.new("CmsPublicationEvents", msg) })
  end
end
