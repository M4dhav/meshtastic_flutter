import 'dart:async';
import 'dart:convert';
import 'dart:io' show Platform;
import 'dart:typed_data';
import 'package:flutter/foundation.dart';
import 'package:flutter_blue_plus/flutter_blue_plus.dart';
import 'package:logging/logging.dart';
import 'package:permission_handler/permission_handler.dart';

import '../generated/mesh.pb.dart';
import '../generated/config.pb.dart';
import '../generated/module_config.pb.dart';
import '../generated/admin.pb.dart';
import '../generated/channel.pb.dart';
import '../generated/portnums.pb.dart';
import 'models/connection_state.dart';
import 'models/mesh_packet_wrapper.dart';
import 'models/node_info.dart';
import 'models/meshtastic_config.dart';
import 'exceptions/meshtastic_exceptions.dart';

/// Main client for communicating with Meshtastic devices over BLE
class MeshtasticClient {
  static final Logger _logger = Logger('MeshtasticClient');

  // Meshtastic BLE Service UUID
  static const String _serviceUuid = '6ba1b218-15a8-461f-9fa8-5dcae273eafd';

  // Characteristic UUIDs
  static const String _toRadioUuid = 'f75c76d2-129e-4dad-a1dd-7866124401e7';
  static const String _fromRadioUuid = '2c55e69e-4993-11ed-b878-0242ac120002';
  static const String _fromNumUuid = 'ed9da18c-a800-4f66-a670-aa7547e34453';

  // Maximum packet size
  static const int _maxPacketSize = 512;

  // Private fields
  BluetoothDevice? _device;
  BluetoothCharacteristic? _toRadioChar;
  BluetoothCharacteristic? _fromRadioChar;
  BluetoothCharacteristic? _fromNumChar;

  StreamSubscription<BluetoothConnectionState>? _connectionSubscription;
  StreamSubscription<List<int>>? _fromNumSubscription;

  final StreamController<ConnectionStatus> _connectionController =
      StreamController<ConnectionStatus>.broadcast();
  final StreamController<MeshPacketWrapper> _packetController =
      StreamController<MeshPacketWrapper>.broadcast();
  final StreamController<NodeInfoWrapper> _nodeController =
      StreamController<NodeInfoWrapper>.broadcast();

  // Configuration and state
  final Map<int, NodeInfoWrapper> _nodes = {};
  MyNodeInfo? _myNodeInfo;
  Config? _config;
  ModuleConfig? _moduleConfig;
  final List<Channel> _channels = [];
  User? _localUser;

  bool _configComplete = false;
  int _expectedFromNum = 0;

  // Public streams
  Stream<ConnectionStatus> get connectionStream => _connectionController.stream;
  Stream<MeshPacketWrapper> get packetStream => _packetController.stream;
  Stream<NodeInfoWrapper> get nodeStream => _nodeController.stream;

  // Controllers
  StreamController<ConnectionStatus> get connectionController => _connectionController;
  StreamController<MeshPacketWrapper> get packetController => _packetController;
  StreamController<NodeInfoWrapper> get nodeController => _nodeController;

  // Getters for current state
  Map<int, NodeInfoWrapper> get nodes => Map.unmodifiable(_nodes);
  MyNodeInfo? get myNodeInfo => _myNodeInfo;
  MeshtasticConfigWrapper? get config =>
      _config != null && _moduleConfig != null
      ? MeshtasticConfigWrapper(
          config: _config!,
          moduleConfig: _moduleConfig!,
          channels: _channels,
        )
      : null;
  User? get localUser => _localUser;
  bool get isConnected => _device?.isConnected ?? false;
  bool get isConfigured => _configComplete;

  /// Initialize the client and request necessary permissions
  Future<void> initialize() async {
    _logger.info('Initializing Meshtastic client');

    // Check if Bluetooth is supported
    if (await FlutterBluePlus.isSupported == false) {
      throw const BluetoothException('Bluetooth not supported on this device');
    }

    // Request permissions
    await _requestPermissions();

    // Check if Bluetooth is enabled
    final state = await FlutterBluePlus.adapterState.first;
    if (state != BluetoothAdapterState.on) {
      throw const BluetoothException('Bluetooth is not enabled');
    }

    _logger.info('Meshtastic client initialized successfully');
  }

  /// Request necessary permissions for BLE
  Future<void> _requestPermissions() async {
    if (!kIsWeb && Platform.isAndroid) {
      // Android 12+ (API 31+): use specific BLE permissions, not legacy Permission.bluetooth
      final connectStatus = await Permission.bluetoothConnect.request();
      if (!connectStatus.isGranted) {
        throw const PermissionException('Permission denied: Permission.bluetoothConnect');
      }
      final scanStatus = await Permission.bluetoothScan.request();
      if (!scanStatus.isGranted) {
        throw const PermissionException('Permission denied: Permission.bluetoothScan');
      }
    } else if (!kIsWeb && Platform.isIOS) {
      // iOS: Permission.bluetooth maps to CBManagerAuthorization
      final bluetoothStatus = await Permission.bluetooth.request();
      if (!bluetoothStatus.isGranted) {
        throw const PermissionException('Permission denied: Permission.bluetooth');
      }
    }

    final locationStatus = await Permission.locationWhenInUse.request();
    if (!locationStatus.isGranted) {
      throw const PermissionException('Permission denied: Permission.locationWhenInUse');
    }
  }

  /// Scan for nearby Meshtastic devices
  Stream<BluetoothDevice> scanForDevices({
    Duration timeout = const Duration(seconds: 10),
  }) async* {
    _logger.info('Scanning for Meshtastic devices');

    final completer = Completer<void>();
    Timer(timeout, () {
      if (!completer.isCompleted) {
        completer.complete();
        FlutterBluePlus.stopScan();
      }
    });

    // Start scanning with service UUID filter
    await FlutterBluePlus.startScan(
      withServices: [Guid(_serviceUuid)],
      timeout: timeout,
    );

    await for (final results in FlutterBluePlus.scanResults) {
      if (completer.isCompleted) break;

      for (final result in results) {
        final device = result.device;
        if (device.platformName.isNotEmpty ||
            result.advertisementData.serviceUuids.contains(
              Guid(_serviceUuid),
            )) {
          _logger.info(
            'Found Meshtastic device: ${device.platformName} (${device.remoteId})',
          );
          yield device;
        }
      }
    }

    if (!completer.isCompleted) {
      completer.complete();
    }
  }

  /// Connect to a specific Meshtastic device
  Future<void> connectToDevice(BluetoothDevice device) async {
    _logger.info(
      'Connecting to device: ${device.platformName} (${device.remoteId})',
    );

    try {
      _emitConnectionState(MeshtasticConnectionState.connecting);

      // Disconnect from any existing device
      await disconnect();

      _device = device;

      // Listen for connection state changes
      _connectionSubscription = device.connectionState.listen((state) {
        if (state == BluetoothConnectionState.disconnected) {
          _handleDisconnection();
        }
      });

      // Connect to device
      await device.connect(timeout: const Duration(seconds: 30));

      // Discover services
      final services = await device.discoverServices();
      final meshtasticService = services.firstWhere(
        (service) =>
            service.uuid.toString().toLowerCase() == _serviceUuid.toLowerCase(),
        orElse: () =>
            throw const ConnectionException('Meshtastic service not found'),
      );

      // Get characteristics
      _toRadioChar = meshtasticService.characteristics.firstWhere(
        (char) =>
            char.uuid.toString().toLowerCase() == _toRadioUuid.toLowerCase(),
        orElse: () =>
            throw const ConnectionException('ToRadio characteristic not found'),
      );

      _fromRadioChar = meshtasticService.characteristics.firstWhere(
        (char) =>
            char.uuid.toString().toLowerCase() == _fromRadioUuid.toLowerCase(),
        orElse: () => throw const ConnectionException(
          'FromRadio characteristic not found',
        ),
      );

      _fromNumChar = meshtasticService.characteristics.firstWhere(
        (char) =>
            char.uuid.toString().toLowerCase() == _fromNumUuid.toLowerCase(),
        orElse: () =>
            throw const ConnectionException('FromNum characteristic not found'),
      );

      // Log characteristic properties for debugging
      _logger.info(
        'ToRadio properties: write=${_toRadioChar!.properties.write}, '
        'writeWithoutResponse=${_toRadioChar!.properties.writeWithoutResponse}',
      );
      _logger.info(
        'FromRadio properties: read=${_fromRadioChar!.properties.read}, '
        'notify=${_fromRadioChar!.properties.notify}',
      );
      _logger.info(
        'FromNum properties: read=${_fromNumChar!.properties.read}, '
        'notify=${_fromNumChar!.properties.notify}',
      );

      // Set MTU to 512 (Android only — iOS negotiates MTU automatically)
      if (!kIsWeb && Platform.isAndroid) {
        await device.requestMtu(512);
      }

      // Enable notifications on FromNum
      await _fromNumChar!.setNotifyValue(true);
      _fromNumSubscription = _fromNumChar!.lastValueStream.listen(
        _handleFromNumNotification,
      );

      _emitConnectionState(MeshtasticConnectionState.configuring);

      // Start configuration process
      await _startConfiguration();

      _logger.info('Successfully connected to device');
    } catch (e) {
      _logger.severe('Failed to connect to device: $e');
      _emitConnectionState(
        MeshtasticConnectionState.error,
        errorMessage: e.toString(),
      );
      rethrow;
    }
  }

  /// Disconnect from the current device
  Future<void> disconnect() async {
    _logger.info('Disconnecting from device');

    await _fromNumSubscription?.cancel();
    _fromNumSubscription = null;

    await _connectionSubscription?.cancel();
    _connectionSubscription = null;

    if (_device?.isConnected == true) {
      await _device!.disconnect();
    }

    _device = null;
    _toRadioChar = null;
    _fromRadioChar = null;
    _fromNumChar = null;

    _configComplete = false;
    _expectedFromNum = 0;
    _nodes.clear();
    _myNodeInfo = null;
    _config = null;
    _moduleConfig = null;
    _channels.clear();
    _localUser = null;

    _emitConnectionState(MeshtasticConnectionState.disconnected);
  }

  /// Send a text message to a specific node or broadcast
  Future<void> sendTextMessage(
    String message, {
    int? destinationId,
    int channel = 0,
  }) async {
    if (!isConnected) {
      throw const ConnectionException('Not connected to a device');
    }

    if (!isConfigured) {
      throw const ConnectionException('Device configuration not complete');
    }

    // Generate a random packet ID
    final packetId = DateTime.now().millisecondsSinceEpoch & 0xFFFFFFFF;

    final packet = MeshPacket(
      from: _myNodeInfo?.myNodeNum ?? 0, // Set sender node ID
      to: destinationId ?? 0xFFFFFFFF, // 0xFFFFFFFF for broadcast
      channel: channel,
      id: packetId,
      decoded: Data(
        portnum: PortNum.TEXT_MESSAGE_APP,
        payload: utf8.encode(message),
      ),
      wantAck: destinationId != null, // Request ACK for direct messages
      hopLimit: 3,
      priority: MeshPacket_Priority.DEFAULT,
    );

    _logger.info(
      'Sending text message: "$message" from ${packet.from.toRadixString(16)} to ${packet.to.toRadixString(16)} on channel $channel',
    );
    await _sendPacket(packet);
  }

  /// Send raw data on a custom port (e.g., PRIVATE_APP) — not intercepted by firmware modules
  /// Returns silently if not ready. If BLE is down, the write throws a
  /// platform exception ("device is disconnected") which the caller handles.
  Future<void> sendData(
    List<int> payload, {
    int portnum = 256, // PRIVATE_APP
  }) async {
    if (_toRadioChar == null) return; // Not ready — skip silently

    final packetId = DateTime.now().millisecondsSinceEpoch & 0xFFFFFFFF;

    final packet = MeshPacket(
      to: 0xFFFFFFFF, // Broadcast
      id: packetId,
      decoded: Data(
        portnum: PortNum.valueOf(portnum) ?? PortNum.PRIVATE_APP,
        payload: payload,
      ),
      hopLimit: 3,
      priority: MeshPacket_Priority.RELIABLE,
    );

    _logger.info('Sending custom data: ${payload.length} bytes on port $portnum');
    await _sendPacket(packet);
  }

  /// Send a position update
  Future<void> sendPosition(
    double latitude,
    double longitude, {
    int? altitude,
  }) async {
    if (!isConnected) {
      throw const ConnectionException('Not connected to a device');
    }

    if (!isConfigured) {
      throw const ConnectionException('Device configuration not complete');
    }

    final position = Position(
      latitudeI: (latitude * 1e7).round(),
      longitudeI: (longitude * 1e7).round(),
      altitude: altitude,
      time: DateTime.now().millisecondsSinceEpoch ~/ 1000,
    );

    // Generate a random packet ID
    final packetId = DateTime.now().millisecondsSinceEpoch & 0xFFFFFFFF;

    final packet = MeshPacket(
      from: _myNodeInfo?.myNodeNum ?? 0, // Set sender node ID
      to: 0xFFFFFFFF, // Broadcast
      id: packetId,
      decoded: Data(
        portnum: PortNum.POSITION_APP,
        payload: position.writeToBuffer(),
      ),
      hopLimit: 3,
      priority: MeshPacket_Priority.DEFAULT,
    );

    _logger.info(
      'Sending position: lat=$latitude, lon=$longitude, alt=$altitude',
    );
    await _sendPacket(packet);
  }

  /// Send an admin config message to the device (e.g., to disable device GPS)
  Future<void> sendAdminConfig(Config config) async {
    if (!isConnected) {
      throw const ConnectionException('Not connected to a device');
    }

    final adminMessage = AdminMessage(setConfig: config);
    final packetId = DateTime.now().millisecondsSinceEpoch & 0xFFFFFFFF;

    final packet = MeshPacket(
      from: _myNodeInfo?.myNodeNum ?? 0,
      to: _myNodeInfo?.myNodeNum ?? 0, // Send to self (local device)
      id: packetId,
      decoded: Data(
        portnum: PortNum.ADMIN_APP,
        payload: adminMessage.writeToBuffer(),
        wantResponse: true,
      ),
      hopLimit: 0, // Local only
      priority: MeshPacket_Priority.RELIABLE,
    );

    _logger.info('Sending admin config');
    await _sendPacket(packet);
  }

  /// Send a packet to the device
  Future<void> _sendPacket(MeshPacket packet) async {
    if (_toRadioChar == null) {
      throw const ConnectionException('ToRadio characteristic not available');
    }

    final toRadio = ToRadio(packet: packet);
    final data = toRadio.writeToBuffer();

    if (data.length > _maxPacketSize) {
      throw const ProtocolException('Packet too large');
    }

    _logger.info(
      'Sending packet: from=${packet.from.toRadixString(16)}, to=${packet.to.toRadixString(16)}, '
      'id=${packet.id}, portnum=${packet.decoded.portnum}, size=${data.length} bytes',
    );

    // Always write WITH response — write-without-response can silently drop packets.
    // The official Python library uses response=True for all ToRadio writes.
    await _toRadioChar!.write(
      data,
      withoutResponse: false,
    );

    _logger.fine('Packet sent successfully');
  }

  /// Start the configuration process
  Future<void> _startConfiguration() async {
    _logger.info('Starting configuration process');

    // Send wantConfigId to start configuration download
    // Use a non-zero random ID — firmware sends back matching configCompleteId
    final configId = DateTime.now().millisecondsSinceEpoch & 0xFFFFFFFF;
    final wantConfig = ToRadio(wantConfigId: configId);
    await _toRadioChar!.write(
      wantConfig.writeToBuffer(),
      withoutResponse: false,
    );

    // Start reading configuration data
    await _readConfiguration();
  }

  /// Read configuration data from the device
  Future<void> _readConfiguration() async {
    _logger.info('Reading configuration from device');

    while (!_configComplete) {
      try {
        final data = await _fromRadioChar!.read();
        if (data.isEmpty) {
          _logger.info('Configuration complete - received empty packet');
          _configComplete = true;
          _emitConnectionState(MeshtasticConnectionState.connected);
          break;
        }

        await _processFromRadioData(data);

        // Small delay to prevent overwhelming the device
        await Future.delayed(const Duration(milliseconds: 50));
      } catch (e) {
        _logger.warning('Error reading configuration: $e');
        // Mark config as complete anyway — BLE is connected, services discovered,
        // notifications enabled. Config read errors shouldn't block packet sending.
        _configComplete = true;
        _emitConnectionState(MeshtasticConnectionState.connected);
        break;
      }
    }
  }

  /// Process incoming data from FromRadio characteristic
  Future<void> _processFromRadioData(List<int> data) async {
    try {
      final fromRadio = FromRadio.fromBuffer(data);
      _logger.fine('Received FromRadio: ${fromRadio.toString()}');

      if (fromRadio.hasMyInfo()) {
        _myNodeInfo = fromRadio.myInfo;
        _logger.info(
          'Received MyNodeInfo: myNodeNum=${_myNodeInfo!.myNodeNum.toRadixString(16)}',
        );
      }

      if (fromRadio.hasNodeInfo()) {
        final nodeInfo = NodeInfoWrapper(fromRadio.nodeInfo);
        _nodes[nodeInfo.num] = nodeInfo;
        _nodeController.add(nodeInfo);
        _logger.info(
          'Received NodeInfo: num=${nodeInfo.num.toRadixString(16)}, '
          'displayName=${nodeInfo.displayName}',
        );

        // Extract user info from the node info
        if (nodeInfo.user != null &&
            _localUser == null &&
            _myNodeInfo != null &&
            nodeInfo.num == _myNodeInfo!.myNodeNum) {
          _localUser = nodeInfo.user;
          _logger.info(
            'Received local User: longName=${_localUser!.longName}, '
            'shortName=${_localUser!.shortName}',
          );
        }
      }

      if (fromRadio.hasConfig()) {
        _config = fromRadio.config;
        _logger.info('Received Config');
      }

      if (fromRadio.hasModuleConfig()) {
        _moduleConfig = fromRadio.moduleConfig;
        _logger.info('Received ModuleConfig');
      }

      if (fromRadio.hasChannel()) {
        final channel = fromRadio.channel;
        if (channel.index < _channels.length) {
          _channels[channel.index] = channel;
        } else {
          while (_channels.length <= channel.index) {
            _channels.add(Channel());
          }
          _channels[channel.index] = channel;
        }
        _logger.info('Received Channel ${channel.index}');
      }

      if (fromRadio.hasPacket()) {
        final packetWrapper = MeshPacketWrapper(fromRadio.packet);
        _packetController.add(packetWrapper);
        _logger.info('Received MeshPacket: ${packetWrapper.toString()}');
      }

      if (fromRadio.hasConfigCompleteId()) {
        _logger.info('Configuration complete');
        _configComplete = true;
        _emitConnectionState(MeshtasticConnectionState.connected);

        // Log summary of received configuration
        _logger.info('Configuration summary:');
        _logger.info('  MyNodeInfo: ${_myNodeInfo != null ? "✓" : "✗"}');
        _logger.info('  Config: ${_config != null ? "✓" : "✗"}');
        _logger.info('  ModuleConfig: ${_moduleConfig != null ? "✓" : "✗"}');
        _logger.info('  Channels: ${_channels.length}');
        _logger.info('  Nodes: ${_nodes.length}');
        _logger.info('  LocalUser: ${_localUser != null ? "✓" : "✗"}');
      }
    } catch (e) {
      _logger.warning('Error processing FromRadio data: $e');
      throw ProtocolException('Failed to parse FromRadio data', e);
    }
  }

  /// Handle FromNum notifications
  void _handleFromNumNotification(List<int> data) {
    if (data.length >= 4) {
      final bytes = Uint8List.fromList(data);
      final fromNum = ByteData.view(bytes.buffer).getUint32(0, Endian.little);
      _logger.fine(
        'FromNum notification: $fromNum (expected: $_expectedFromNum)',
      );

      if (fromNum > _expectedFromNum) {
        _expectedFromNum = fromNum;
        // Read new data from FromRadio
        _readFromRadio();
      }
    }
  }

  /// Read available data from FromRadio
  Future<void> _readFromRadio() async {
    try {
      while (true) {
        final data = await _fromRadioChar!.read();
        if (data.isEmpty) break;

        await _processFromRadioData(data);
      }
    } catch (e) {
      _logger.warning('Error reading from FromRadio: $e');
    }
  }

  /// Handle disconnection
  void _handleDisconnection() {
    _logger.info('Device disconnected');
    _emitConnectionState(MeshtasticConnectionState.disconnected);
  }

  /// Emit connection state change
  void _emitConnectionState(
    MeshtasticConnectionState state, {
    String? errorMessage,
  }) {
    final status = ConnectionStatus(
      state: state,
      deviceAddress: _device?.remoteId.toString(),
      deviceName: _device?.platformName,
      errorMessage: errorMessage,
      timestamp: DateTime.now(),
    );

    _connectionController.add(status);
  }

  /// Dispose of the client and clean up resources
  void dispose() {
    _logger.info('Disposing Meshtastic client');

    disconnect();
    _connectionController.close();
    _packetController.close();
    _nodeController.close();
  }
}
