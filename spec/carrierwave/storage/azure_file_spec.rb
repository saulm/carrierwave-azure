require 'spec_helper'

describe CarrierWave::Storage::Azure::File do
  class TestUploader < CarrierWave::Uploader::Base
    storage :azure
  end

  let(:uploader) { TestUploader.new }
  let(:storage)  { CarrierWave::Storage::Azure.new uploader }

  before do
    allow(uploader).to receive(:azure_container).and_return('test')
  end

  describe '#url' do
    before do
      allow_any_instance_of(CarrierWave::Storage::Azure::File).to receive(:private_container?).and_return(false)
    end

    context 'with storage_blob_host' do
      before do
        allow(uploader).to receive(:azure_storage_blob_host).and_return('http://example.com')
      end

      subject { CarrierWave::Storage::Azure::File.new(uploader, storage.connection, 'dummy.txt').url }

      it 'should return on asset_host' do
        expect(subject).to eq "http://example.com/test/dummy.txt"
      end
    end

    context 'with asset_host' do
      before do
        allow(uploader).to receive(:asset_host).and_return('http://example.com')
      end

      subject { CarrierWave::Storage::Azure::File.new(uploader, storage.connection, 'dummy.txt').url }

      it 'should return on asset_host' do
        expect(subject).to eq "http://example.com/test/dummy.txt"
      end
    end
  end

  describe '#exists?' do
    context 'when blob file does not exist' do
      before do
        allow(storage.connection).to receive(:get_blob).and_return(nil)
      end

      subject { CarrierWave::Storage::Azure::File.new(uploader, storage.connection, 'dummy.txt').exists? }

      it 'should return false' do
        expect(subject).to eql false
      end
    end
  end

  context 'with expire time options' do
    before do
      @now = Time.now
      allow(uploader).to receive(:azure_container).and_return(ENV['PRIVATE_CONTAINER_NAME'] || 'private')
      allow_any_instance_of(::Azure::Core::Auth::SharedAccessSignature).to receive(:create_query_values).and_return({ sig: 'sharedsignature' })
    end

    context 'with storage blob host' do
      before do
        allow(uploader).to receive(:azure_storage_blob_host).and_return('http://example.com')
        @subject = CarrierWave::Storage::Azure::File.new(uploader, storage.connection, 'dummy.png').url({ expiry: 10.minutes })
        @expected_url = "http://example.com/#{uploader.azure_container}/dummy.png?sig=sharedsignature"
      end

      it 'should return URL with SAS query string' do
        expect(@subject).to eq(@expected_url)
      end
    end
  end
end
