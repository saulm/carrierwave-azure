require 'azure'
require 'azure/storage'
require 'azure/blob/auth/shared_access_signature'
require 'concurrent'

module CarrierWave
  module Storage
    class Azure < Abstract
      def store!(file)
        azure_file = CarrierWave::Storage::Azure::File.new(uploader, connection, uploader.store_path)
        azure_file.store!(file)
        azure_file
      end

      def retrieve!(identifer)
        CarrierWave::Storage::Azure::File.new(uploader, connection, uploader.store_path(identifer))
      end

      def connection
        client = ::Azure::Storage::Client.create(:storage_account_name => @uploader.azure_storage_account_name, :storage_access_key => @uploader.azure_storage_access_key)
        client.blob_client
      end

      class File
        attr_reader :path

        def initialize(uploader, connection, path)
          @uploader = uploader
          @connection = connection
          @path = path
        end

        def store!(file)
          if file.size < 50000000 #50MB
            @content = file.read
            @content_type = file.content_type
            @connection.create_block_blob(@uploader.azure_container, @path, @content, "content_type" => @content_type, "x-ms-version" => "2016-05-31")
          else
            chunked_upload(file)
          end
          true
        end

        def chunked_upload(file)
          begin
            @connection.delete_blob(@uploader.azure_container, @path)
          rescue
          end
          blocks = []
          pool = ::Concurrent::FixedThreadPool.new(20)

          #Ugly but http://www.rubydoc.info/github/jnicklas/carrierwave/CarrierWave/SanitizedFile#read-instance_method
          file_contents = file.read

          chunk_size = 2000000
          (0..file_contents.size).step(chunk_size).each do |step|
            chunk = file_contents.byteslice(step..step+chunk_size-1)
            block_id = Base64.strict_encode64(::SecureRandom.uuid)
            blocks << [block_id, :uncommited]

            pool.post do
              @connection.put_blob_block(@uploader.azure_container, @path, block_id, chunk)
            end
          end
          pool.shutdown
          pool.wait_for_termination
          @connection.commit_blob_blocks(@uploader.azure_container, @path, blocks)
        end

        def url(options = {})
          _path = ::File.join(@uploader.azure_container, @path)
          _url = if private_container?
                   signed_url(_path, options.slice(:expiry))
                 else
                   public_url(_path, options)
                 end
          _url
        end

        def read
          content
        end

        def content_type
          @content_type = blob.properties[:content_type] if @content_type.nil? && !blob.nil?
          @content_type
        end

        def content_type=(new_content_type)
          @content_type = new_content_type
        end

        def exists?
          @connection.list_blobs(@uploader.azure_container).collect{|b|b.name}.include? @path
        end

        def size
          blob.properties[:content_length] unless blob.nil?
        end

        def filename
          URI.decode(url).gsub(/.*\/(.*?$)/, '\1')
        end

        def extension
          @path.split('.').last
        end

        def delete
          begin
            @connection.delete_blob(@uploader.azure_container, @path)
            true
          rescue ::Azure::Core::Http::HTTPError
            false
          end
        end

        private

        def blob
          load_content if @blob.nil?
          @blob
        end

        def content
          load_content if @content.nil?
          @content
        end

        def load_content
          @blob, @content = begin
            @connection.get_blob(@uploader.azure_container, @path)
          rescue ::Azure::Core::Http::HTTPError
          end
        end

        def get_container_acl(container_name, options = {})
          begin
            acl_data = @connection.get_container_acl(container_name, options)
            acl = if acl_data.is_a?(Array)
                    acl_data.size > 0 ? acl_data[0] : nil
                  else
                    acl_data
                  end
            acl
          rescue ::Azure::Core::Http::HTTPError => exception
            puts "#{self.class.name}.get_container_acl raised HTTPError exception, with reason:\n #{exception.message}"
            nil
          end
        end

        def sign(path, options = {})
          uri = @connection.generate_uri(path)
          account = @uploader.azure_storage_account_name
          secret_key = @uploader.azure_storage_access_key
          ::Azure::Blob::Auth::SharedAccessSignature.new(account, secret_key).signed_uri(uri, options)
        end

        def private_container?
          acl = get_container_acl(@uploader.azure_container, {} )
          acl && acl.public_access_level.nil?
        end

        def signed_url(path, options = {})
          expiry = options[:expiry] ? (Time.now.to_i + options[:expiry].to_i) : nil
          _options = { permissions: 'r', resource: 'b' }
          _options[:expiry] = Time.at(expiry).utc.iso8601 if expiry
          sign( path, options.merge!(_options) ).to_s
        end

        def public_url(path, options = {})
          @connection.generate_uri(path).to_s
        end
      end
    end
  end
end
