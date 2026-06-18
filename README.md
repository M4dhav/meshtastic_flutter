# Meshtastic Flutter

A comprehensive Flutter package for communicating with Meshtastic devices over Bluetooth Low Energy (BLE).

## Features

- **Complete BLE Integration**: Connect to Meshtastic devices using the official Meshtastic Bluetooth service
- **Full Protocol Support**: Implements the complete Meshtastic protocol including configuration, messaging, and telemetry
- **Node Management**: Track and manage all nodes in the mesh network
- **Real-time Updates**: Stream-based API for real-time packet and node updates
- **Type-safe**: Fully typed API with comprehensive wrapper classes
- **Cross-platform**: Works on both Android and iOS

## Installation

Add this to your package's `pubspec.yaml` file:

```yaml
dependencies:
  meshtastic_flutter: ^0.0.1
```

Complete the setup for `permission_handler` plugin as given [here](https://pub.dev/packages/permission_handler)

## Permissions

### Android

Add these permissions to your `android/app/src/main/AndroidManifest.xml`:

```xml
<uses-permission android:name="android.permission.BLUETOOTH" />
<uses-permission android:name="android.permission.BLUETOOTH_ADMIN" />
<uses-permission android:name="android.permission.BLUETOOTH_CONNECT" />
<uses-permission android:name="android.permission.BLUETOOTH_SCAN" />
<uses-permission android:name="android.permission.ACCESS_COARSE_LOCATION" />
<uses-permission android:name="android.permission.ACCESS_FINE_LOCATION" />
```

### iOS

Add these to your `ios/Runner/Info.plist`:

```xml
<key>NSBluetoothAlwaysUsageDescription</key>
<string>This app needs Bluetooth to connect to Meshtastic devices</string>
<key>NSBluetoothPeripheralUsageDescription</key>
<string>This app needs Bluetooth to connect to Meshtastic devices</string>
<key>NSLocationWhenInUseUsageDescription</key>
<string>This app needs location for Bluetooth scanning</string>
```

## Quick Start

```dart
import 'package:meshtastic_flutter/meshtastic_flutter.dart';

void main() async {
  // Create and initialize the client
  final client = MeshtasticClient();
  await client.initialize();

  // Listen for connection changes
  client.connectionStream.listen((status) {
    print('Connection: ${status.state}');
  });

  // Listen for incoming messages
  client.packetStream.listen((packet) {
    if (packet.isTextMessage) {
      print('Message from ${packet.from.toRadixString(16)}: ${packet.textMessage}');
    }
  });

  // Scan and connect to a device
  await for (final device in client.scanForDevices()) {
    await client.connectToDevice(device);
    break; // Connect to first device found
  }

  // Send a message
  await client.sendTextMessage('Hello Mesh!');

  // Send position
  await client.sendPosition(37.7749, -122.4194);
}
```

## API Documentation

### MeshtasticClient

The main client class for interacting with Meshtastic devices.

#### Methods

- `initialize()` - Initialize the client and request permissions
- `scanForDevices()` - Stream of nearby Meshtastic devices
- `connectToDevice(BluetoothDevice)` - Connect to a specific device
- `disconnect()` - Disconnect from current device
- `sendTextMessage(String, {int? destinationId, int channel})` - Send text message
- `sendPosition(double lat, double lon, {int? altitude})` - Send position update

#### Properties

- `connectionStream` - Stream of connection status changes
- `packetStream` - Stream of incoming mesh packets
- `nodeStream` - Stream of node information updates
- `nodes` - Map of all known nodes
- `myNodeInfo` - Information about the local node
- `config` - Device configuration
- `isConnected` - Whether connected to a device
- `isConfigured` - Whether initial configuration is complete

### MeshPacketWrapper

Wrapper class for MeshPacket with convenience methods.

#### Properties

- `from` / `to` - Sender and destination node IDs
- `channel` - Channel number
- `isTextMessage` / `isTelemetry` / `isPosition` - Packet type checks
- `textMessage` - Text content (if text message)
- `isEncrypted` / `isDecoded` - Encryption status
- `packetTypeDescription` - Human-readable packet type

#### Position Data (from packet payload)

The following properties decode position data directly from the packet payload, ensuring you get the exact position from each specific packet rather than the node's current position:

- `positionData` - Decoded `Position` protobuf object from packet payload
- `latitude` - Latitude in decimal degrees (null if not a position packet)
- `longitude` - Longitude in decimal degrees (null if not a position packet)
- `altitude` - Altitude in meters (null if not available)
- `timestamp` - Timestamp from position data (null if not available)

### NodeInfoWrapper

Wrapper class for NodeInfo with enhanced functionality.

#### Properties

- `displayName` - Best available name for the node
- `longName` / `shortName` - User names
- `latitude` / `longitude` / `altitude` - Position data
- `batteryLevel` / `voltage` - Power information
- `lastHeard` - Last contact timestamp
- `isOnline` - Whether node is currently reachable
- `statusDescription` - Summary status string

### ConnectionStatus

Information about the current connection state.

#### Properties

- `state` - Current connection state (disconnected, connecting, configuring, connected, error)
- `deviceAddress` / `deviceName` - Connected device information
- `errorMessage` - Error description (if any)
- `timestamp` - When status changed

## Usage Examples

### Basic Messaging

```dart
final client = MeshtasticClient();
await client.initialize();

// Connect to first device found
await for (final device in client.scanForDevices()) {
  await client.connectToDevice(device);
  break;
}

// Send broadcast message
await client.sendTextMessage('Hello everyone!');

// Send direct message to specific node
await client.sendTextMessage('Private message', destinationId: 0x12345678);

// Send on specific channel
await client.sendTextMessage('Channel message', channel: 1);
```

### Position Tracking

```dart
// Listen for position packets and extract position data from each packet
client.packetStream.listen((packet) {
  if (packet.isPosition) {
    // Extract position directly from packet payload
    // This gives you the exact position from THIS packet, not the node's current position
    if (packet.latitude != null && packet.longitude != null) {
      print('Position from ${packet.from.toRadixString(16)}:');
      print('  Lat: ${packet.latitude}');
      print('  Lon: ${packet.longitude}');
      print('  Alt: ${packet.altitude}m');
      print('  Time: ${packet.timestamp}');

      // Save to database with packet-specific position
      savePositionToDatabase(
        deviceId: packet.from,
        latitude: packet.latitude!,
        longitude: packet.longitude!,
        altitude: packet.altitude,
        timestamp: packet.timestamp,
      );
    }
  }
});
```

### Node Monitoring

```dart
// Listen for node updates
client.nodeStream.listen((node) {
  print('Node ${node.displayName}:');
  print('  Battery: ${node.batteryLevel}%');
  print('  Last heard: ${node.lastHeard}');
  print('  Position: ${node.latitude}, ${node.longitude}');
  print('  Distance: ${myNode.distanceFrom(node)}m');
});

// Get all nodes
for (final node in client.nodes.values) {
  print('${node.displayName} - ${node.statusDescription}');
}
```

### Configuration Access

```dart
final config = client.config;
if (config != null) {
  print('Device role: ${config.deviceRole}');
  print('LoRa region: ${config.region}');
  print('Channels: ${config.channels.length}');
  print('Primary channel: ${config.primaryChannel?.settings.name}');
}
```

### Error Handling

```dart
try {
  await client.connectToDevice(device);
} on BluetoothException catch (e) {
  print('Bluetooth error: $e');
} on ConnectionException catch (e) {
  print('Connection error: $e');
} on PermissionException catch (e) {
  print('Permission error: $e');
} catch (e) {
  print('Unexpected error: $e');
}
```

## Protocol Implementation

This package implements the complete Meshtastic Bluetooth protocol:

### BLE Service Details

- **Service UUID**: `6ba1b218-15a8-461f-9fa8-5dcae273eafd`
- **ToRadio**: `f75c76d2-129e-4dad-a1dd-7866124401e7` (write commands/packets)
- **FromRadio**: `2c55e69e-4993-11ed-b878-0242ac120002` (read responses)
- **FromNum**: `ed9da18c-a800-4f66-a670-aa7547e34453` (notifications for new data)

### Configuration Flow

1. Connect to device and discover services
2. Set MTU to 512 bytes
3. Enable notifications on FromNum characteristic
4. Send `wantConfigId` to start configuration download
5. Read configuration data until `configCompleteId` received
6. Continue reading for ongoing mesh packets

### Supported Message Types

- Text messages (`TEXT_MESSAGE_APP`)
- Position updates (`POSITION_APP`)
- Node information (`NODEINFO_APP`)
- Telemetry data (`TELEMETRY_APP`)
- Administrative commands (`ADMIN_APP`)
- Routing information (`ROUTING_APP`)

## Contributing

Contributions are welcome! Please read our contributing guidelines and submit pull requests to our repository.

## License

This project is licensed under the GPL-3.0 License - see the LICENSE file for details.

## Related Projects

- [Meshtastic](https://meshtastic.org/) - The main Meshtastic project
- [Meshtastic Android](https://github.com/meshtastic/Meshtastic-Android) - Official Android app
- [Meshtastic Protobuf](https://github.com/meshtastic/protobufs) - Protocol buffer definitions
