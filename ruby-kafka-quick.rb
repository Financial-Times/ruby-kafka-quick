require "poseidon"

consumer = Poseidon::PartitionConsumer.new("methode-article-image-set-mapper", "ip-172-24-45-243.eu-west-1.compute.internal", 9092, "NativeCmsPublicationEvents", 0, :latest_offset)

loop do
  messages = consumer.fetch
  messages.each do |m|
    puts m.value
  end
end
