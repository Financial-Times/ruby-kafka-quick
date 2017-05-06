FROM ruby:alpine

RUN gem install poseidon
ADD ruby-kafka-quick.rb .

CMD ruby ruby-kafka-quick.rb
