require 'azure'

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
        @connection ||= begin
          %i(storage_account_name storage_access_key storage_blob_host).each do |key|
            ::Azure.config.send("#{key}=", uploader.send("azure_#{key}"))
          end
          ::Azure::Blob::BlobService.new
        end
      end

      class File
        SAS_DEFAULT_EXPIRE_TIME = 30.seconds

        attr_reader :path

        def initialize(uploader, connection, path)
          @uploader = uploader
          @connection = connection
          @path = path
        end

        def store!(file)
          @content = file.read
          @content_type = file.content_type
          @connection.create_block_blob(@uploader.azure_container, @path, @content, content_type: @content_type)
          true
        end

        def url(options = {})
          _path = ::File.join(@uploader.azure_container, @path)
          _url = if private_container?
                   signed_url(_path, options.slice(:start, :expiry, :permissions))
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
          !blob.nil?
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
          uri = if @uploader.asset_host
                  URI("#{@uploader.asset_host}/#{path}")
                else
                  @connection.generate_uri(path)
                end
          account = @uploader.send(:azure_storage_account_name)
          secret_key = @uploader.send(:azure_storage_access_key)
          ::Azure::Blob::Auth::SharedAccessSignature.new(account, secret_key)
                                                    .signed_uri(uri, options)
        end

        def private_container?
          acl = get_container_acl( @uploader.send(:azure_container), {} )
          acl && acl.public_access_level.nil?
        end

        def signed_url(path, options = {})
          now = options[:start] ? options[:start].to_i : Time.now.to_i
          expire_time = now + (options[:expiry] ? options[:expiry].to_i : SAS_DEFAULT_EXPIRE_TIME)
          _options = { permissions: 'r',
                       resource: 'b',
                       expiry: Time.at(expire_time).utc.iso8601 }

          sign( path, options.merge!(_options) ).to_s
        end

        def public_url(path, options = {})
          if @uploader.asset_host
            "#{@uploader.asset_host}/#{path}"
          else
            @connection.generate_uri(path).to_s
          end
        end
      end
    end
  end
end
