require 'aws-sdk'

class Bucket
  FETCH_SIZE = 100
  def initialize(config, from_bucket_name, to_bucket_name, thread = 10)
    @thread = thread.to_i
    @config = config
    @from_bucket = AWS::S3.new(@config).buckets[from_bucket_name]
    @to_bucket   = AWS::S3.new(@config).buckets[to_bucket_name]
    @from_objects = @from_bucket.objects.with_prefix('products/')
    @current_items = @from_objects.page(per_page: FETCH_SIZE)
    @mutex = Mutex.new
  end

  def sync
    threads = []
    @thread.times do |i|
      threads << Thread.new(i) do |t|
        while object = fetch_object
          copy(object)
        end
      end
    end
    threads.each do |t|
      t.join
    end
  end

  def fetch_object
    if @current_items.empty? && !@current_items.last_page?
      @mutex.synchronize do
        @current_items = @from_objects.page(per_page: FETCH_SIZE, next_token: @current_items.next_token)
      end
    end
    @current_items.shift
  end

  # Copy_from method info
  # Ref: http://docs.aws.amazon.com/AWSRubySDK/latest/AWS/S3/S3Object.html#copy_from-instance_method
  def copy(from_object)
    to_object = nil
    to_object = @to_bucket.objects[from_object.key]

    if to_object.exists? && from_object.content_length == to_object.content_length
      puts "Already exists : #{from_object.key}"
    else
      to_object.copy_from(from_object)
      to_object.acl = :public_read
      puts "Copy object    : #{from_object.key}"
    end
  end
end

if __FILE__ == $0
  thread = ARGV[0] || 10
  config = {
    access_key_id: "ACCESS_KEY_ID",
    secret_access_key: "SECRET_ACCESS_KEY"
  }
  from = 'FROM_BUCKET_NAME'
  to   = 'TO_BUCKET_NAME'
  b = Bucket.new(config, from , to, thread)
  b.sync
end
