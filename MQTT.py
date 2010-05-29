from twisted.internet.protocol import Protocol

class MQTTProtocol(Protocol):
    buffer = bytearray()
    type = None
    qos = None
    length = None
    messageID = 1
    
    def dataReceived(self, data):
        self._accumulatePacket(data)
        
    def _accumulatePacket(self, data):
        self.buffer.extend(data)
        
        while len(self.buffer):
            if self.length is None:
                # Start on a new packet
                
                # Haven't got enough data to start a new packet,
                # wait for some more
                if len(self.buffer) < 2: break 
                
                lenLen = 1
                while lenLen < len(self.buffer):
                    if not self.buffer[lenLen] & 0x80: break
                    lenLen += 1
                
                # We still haven't got all of the remaining length field
                if lenLen < len(self.buffer) and self.buffer[lenLen] & 0x80: return
                
                self.type = self.buffer[0] & 0xF0
                self.qos = self.buffer[0] & 0x06
                self.length = self._decodeLength(self.buffer[1:1+lenLen])
                self.buffer = self.buffer[lenLen+1:]
                
            if len(self.buffer) >= self.length:
                chunk = self.buffer[:self.length]
                self._processPacket(chunk)
                self.buffer = self.buffer[self.length:]
                self.length = None
                self.type = None
                self.qos = None
            else:
                break
                
    def _processPacket(self, packet):
        if self.type == 0x01 << 4:
            # Connect
            pass
        elif self.type == 0x02 << 4:
            # Connack
            status = packet[1] 
            self.connackReceived(status)
        elif self.type == 0x03 << 4:
            # Publish
            topicLen = packet[0] * 256 + packet[1]
            topic = self._decodeString(packet)
            packet = packet[2:]
            packet = packet[topicLen:]
            
            if self.qos == 0x00 << 1:    
                payload = bytearray(packet)
            else:
                payload = bytearray(packet[:-2])
                
            self.publishReceived(str(topic), str(payload))    
            
        elif self.type == 0x04 << 4:
            # Puback
            pass
        
        elif self.type == 0x05 << 4:
            # Pubrec
            pass
        
        elif self.type == 0x06 << 4:
            # Pubrel
            pass
        
        elif self.type == 0x07 << 4:
            # Pubcomp
            pass
        
        elif self.type == 0x08 << 4:
            # Subscribe
            pass
        
        elif self.type == 0x09 << 4:
            # Suback
            pass
        
        elif self.type == 0x0A << 4:
            # Unsubscribe
            pass
        
        elif self.type == 0x0B <<4:
            # Unsuback
            pass
        
        elif self.type == 0x0C << 4:
            # Pingreq
            self.pingReqReceived()
            
        elif self.type == 0x0D << 4:
            # Pingresp
            pass
        
        elif self.type == 0x0E << 4:
            pass
            # Disconnect
        
    def connectionMade(self):
        pass
    
    def connectionLost(self, reason):
        pass
    
    def connackReceived(self, status):
        pass
        
    def publishReceived(self, topic, message):
        pass
    
    def pingReqReceived(self):
        pass        
    
    def connect(self, clientID, keepalive = 3000, willTopic = None,
                willMessage = None, willQoS = 0, willRetain = False):
        header = bytearray()
        varHeader = bytearray()
        payload = bytearray()
        
        varHeader.extend( self._encodeString("MQIsdp") )
        varHeader.append(3)
        
        if willMessage is None or willTopic is None:
            # Clean start, no will message
            varHeader.append( 0 << 2 | 1 << 1 )
        else: 
            varHeader.append(willRetain << 5 | willQoS << 3 | 1 << 2 | 1 << 1)
        
        varHeader.extend(self._encodeValue(keepalive/1000))
        
        payload.extend(self._encodeString(clientID))
        if willMessage is not None and willTopic is not None:
            payload.extend(self._encodeString(willTopic))
            payload.extend(self._encodeString(willMessage))
        
        header.append(0x01 << 4)
        header.extend(self._encodeLength(len(varHeader) + len(payload)))
        
        self.transport.write(str(header))
        self.transport.write(str(varHeader))
        self.transport.write(str(payload))
    
    def publish(self, topic, message, qosLevel = 0):
        """
        Only supports QoS level 0 publishes
        """
        header = bytearray()
        varHeader = bytearray()
        payload = bytearray()
        
        # Type = publish, QoS = 0
        header.append(0x03 << 4 | 0x00 << 1)
        
        varHeader.extend(self._encodeString(topic))
        payload.extend(message)
        
        header.extend(self._encodeLength(len(varHeader) + len(payload)))
        
        self.transport.write(str(header))
        self.transport.write(str(varHeader))
        self.transport.write(str(payload))
        
    def subscribe(self, topic, requestedQoS = 0):

        """
        Only supports QoS = 0 subscribes
        Only supports one subscription per message
        """
        header = bytearray()
        varHeader = bytearray()
        payload = bytearray()
        
        # Type = subscribe, QoS = 1
        header.append(0x08 << 4 | 0x01 << 1)
        
        varHeader.extend(self._encodeValue(self.messageID))
        self.messageID += 1
        
        payload.extend(self._encodeString(topic))
        payload.append(0)
        
        header.extend(self._encodeLength(len(varHeader) + len(payload)))
        
        self.transport.write(str(header))
        self.transport.write(str(varHeader))
        self.transport.write(str(payload))
    
    def unsubscribe(self, topic):
        header = bytearray()
        varHeader = bytearray()
        payload = bytearray
        
        header.append(0x0A << 4 | 0x01 < 1)
        
        varHeader.extend(self._encodeValue(self.messageID))
        self.messageID += 1
        
        payload.extend(self._encodeString(topic))
        
        header.extend(self._encodeLength(len(payload) + len(varHeader)))
        
        self.transport.write(str(header))
        self.transport.write(str(varHeader))
        self.transport.write(str(payload))
    
    def pingRequest(self):
        self.transport.write(str(0x0C << 4))
    
    def disconnect(self):
        self.transport.write(str(0x0E << 4))
    
    def _encodeString(self, string):
        encoded = bytearray()
        encoded.append(len(string) >> 8)
        encoded.append(len(string) & 0xFF)
        for i in string:
            encoded.append(i)
        
        return encoded
    
    def _decodeString(self, encodedString):
        length = 256 * encodedString[0] + encodedString[1]
        return str(encodedString[2:2+length])
    
    def _encodeLength(self, length):
        encoded = bytearray()
        while True:
            digit = length % 128
            length //= 128
            if length > 0:
                digit |= 128
            
            encoded.append(digit)
            if length <= 0: break
        
        return encoded
    
    def _encodeValue(self, value):
        encoded = bytearray()
        encoded.append(value >> 8)
        encoded.append(value & 0xFF)
        
        return encoded
    
    def _decodeLength(self, lengthArray):
        length = 0
        multiplier = 1
        for i in lengthArray:
            length += (i & 0x7F) * multiplier
            multiplier *= 0x80
        
        return length
        
