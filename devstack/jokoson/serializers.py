from rest_framework import serializers
from django.contrib.auth.models import User
from .models import Device, Order, DEVICE_STYLE_CHOICES


#class DeviceSerializer(serializers.Serializer):
#    sn = serializers.CharField(read_only=True)
#    public = serializers.BooleanField(required=False)
#    style = serializers.ChoiceField(choices=DEVICE_STYLE_CHOICES, default='basic')
#
#    def create(self, validated_data):
#        """
#        Create and return a new `Device` instance, given the validated data.
#        """
#        return Device.objects.create(**validated_data)
#
#    def update(self, instance, validated_data):
#        """
#        Update and return an existing `Device` instance, given the validated data.
#        """
#        instance.public = validated_data.get('public', instance.public)
#        instance.style = validated_data.get('style', instance.style)
#        instance.save()
#        return instance

class DeviceSerializer(serializers.HyperlinkedModelSerializer):
    owner = serializers.ReadOnlyField(source='owner.username')
    class Meta:
        model = Device
        fields = ('url', 'id', 'sn', 'public', 'style', 'created_date', 'updated_date', 'owner')


class UserSerializer(serializers.HyperlinkedModelSerializer):
        devices = serializers.HyperlinkedRelatedField(many=True, view_name='device-detail', read_only=True)
        class Meta:
            model = User
            fields = ('url', 'id', 'username', 'devices')
