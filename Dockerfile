FROM ruby:2.1.10

RUN gem install poseidon
RUN gem install json
RUN gem install nokogiri
ADD ruby-kafka-quick.sh .
ADD ruby-kafka-quick.rb .

ENTRYPOINT ["/ruby-kafka-quick.sh"]
