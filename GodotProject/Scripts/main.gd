extends Control

const COLOR_BG = Color("#0d1117")
const COLOR_CARD = Color("#161b22")
const COLOR_PRIMARY = Color("#58a6ff")
const COLOR_SECONDARY = Color("#238636")
const COLOR_TEXT = Color("#e6edf3")
const COLOR_TEXT_SECONDARY = Color("#7d8590")
const COLOR_SUCCESS = COLOR_SECONDARY
const COLOR_ONLINE = COLOR_SUCCESS
const COLOR_OFFLINE = Color("#484f58")
const COLOR_WARNING = Color("#da3633") 
const COLOR_INPUT = Color("#21262d")
const COLOR_HOVER = Color("#30363d")
const COLOR_ACTIVE = COLOR_PRIMARY
const COLOR_SELF_MSG = Color("#1f6feb")
const COLOR_OTHER_MSG = COLOR_CARD
const ANIM_SPEED = 0.15
const BUTTON_SCALE = Vector2(1.02, 1.02)
const MAX_FILE_SIZE = 5 * 1024 * 1024
const ALLOWED_EXTENSIONS = ["png", "jpg", "jpeg", "gif", "mp4", "pdf"]

var _client = WebSocketPeer.new()
var server_url = "ws://localhost:8000/ws"
var http_url = "http://localhost:8000"
var user_id: String
var username: String
var password: String
var authenticated = false
var current_call: Dictionary = {}
var call_status: String = "idle"
var waiting_for_response = false
var selected_user_id: String = ""
var peer_connection: WebRTCPeerConnection
var data_channel: WebRTCDataChannel
var multiplayer_peer: WebRTCMultiplayerPeer
var peer_id: int = 0
var ice_servers = [
	{"urls": ["stun:stun.l.google.com:19302"]},
	{"urls": ["stun:stun1.l.google.com:19302"]},
	{"urls": ["stun:stun2.l.google.com:19302"]}
]
var all_users: Array = []
var filtered_users: Array = []
var chat_partners: Array = []
var auth_buttons_enabled = true
var last_sender = null
var last_is_self = null
var file_dialog = FileDialog.new()
var current_upload_file: String = ""
var current_upload_name: String = ""

@onready var http_request = $HTTPRequest
@onready var auth_panel = $AuthPanel
@onready var main_panel = $MainPanel
@onready var call_panel = $CallPanel
@onready var incoming_call_panel = $IncomingCallPanel
@onready var username_input = $AuthPanel/CenterContainer/AuthBox/FormContainer/UsernameInput
@onready var password_input = $AuthPanel/CenterContainer/AuthBox/FormContainer/PasswordInput
@onready var status_text = $AuthPanel/CenterContainer/AuthBox/StatusText
@onready var user_list = $MainPanel/MarginContainer/HSplitContainer/LeftPanel/VBoxContainer/UserContainer/UserList
@onready var chat_display = $MainPanel/MarginContainer/HSplitContainer/RightPanel/VBoxContainer2/MessageContainer/ChatDisplay
@onready var message_input = $MainPanel/MarginContainer/HSplitContainer/RightPanel/VBoxContainer2/InputContainer/MessageInput
@onready var status_label = $MainPanel/MarginContainer/HSplitContainer/LeftPanel/VBoxContainer/StatusContainer/StatusLabel
@onready var call_button = $MainPanel/MarginContainer/HSplitContainer/RightPanel/CallButton
@onready var hangup_button = $CallPanel/CallControls/HangupButton
@onready var accept_call_button = $IncomingCallPanel/CallerInfo/CallButtons/AcceptButton
@onready var reject_call_button = $IncomingCallPanel/CallerInfo/CallButtons/RejectButton
@onready var call_status_label = $CallPanel/CallControls/CallStatus
@onready var caller_label = $IncomingCallPanel/CallerInfo/CallerLabel
@onready var login_button = $AuthPanel/CenterContainer/AuthBox/ButtonContainer/LoginButton
@onready var register_button = $AuthPanel/CenterContainer/AuthBox/ButtonContainer/RegisterButton
@onready var search = $MainPanel/MarginContainer/HSplitContainer/LeftPanel/VBoxContainer/Search

func _ready():
	_setup_ui()
	
	auth_panel.show()
	main_panel.hide()
	call_panel.hide()
	incoming_call_panel.hide()
	
	status_text.text = "Войдите в систему"
	status_label.text = "Статус: Отключено"
	
	login_button.pressed.connect(_on_login_button_pressed)
	register_button.pressed.connect(_on_register_button_pressed)
	message_input.text_submitted.connect(_on_message_submitted)
	call_button.pressed.connect(_on_call_button_pressed)
	hangup_button.pressed.connect(_on_hangup_button_pressed)
	accept_call_button.pressed.connect(_on_accept_call_pressed)
	reject_call_button.pressed.connect(_on_reject_call_pressed)
	http_request.request_completed.connect(_on_http_request_request_completed)
	user_list.item_selected.connect(_on_user_selected)
	search.text_changed.connect(_on_search_text_changed)
	search.text_submitted.connect(_on_search_submitted)
	chat_display.meta_clicked.connect(_on_meta_clicked)
	
	file_dialog.file_mode = FileDialog.FILE_MODE_OPEN_FILE
	file_dialog.access = FileDialog.ACCESS_FILESYSTEM
	file_dialog.filters = PackedStringArray(["*.png, *.jpg, *.jpeg, *.gif, *.mp4, *.pdf"])
	file_dialog.file_selected.connect(_on_file_selected)
	add_child(file_dialog)
	
	var file_button = Button.new()
	file_button.text = "Отправить файл"
	file_button.pressed.connect(_on_file_button_pressed)
	$MainPanel/MarginContainer/HSplitContainer/RightPanel/VBoxContainer2/InputContainer.add_child(file_button)

func _process(delta):
	if _client == null:
		return
	
	_client.poll()
	
	var state = _client.get_ready_state()
	if state == WebSocketPeer.STATE_OPEN:
		if not authenticated:
			var auth_data = {
				"username": username,
				"password": password
			}
			_client.send_text(JSON.stringify(auth_data))
			authenticated = true
			status_text.text = "Аутентификация..."
		
		while _client.get_available_packet_count() > 0:
			var packet = _client.get_packet()
			var message = packet.get_string_from_utf8()
			var data = JSON.parse_string(message)
			
			if data == null:
				print("Ошибка в получении JSON")
				continue
			
			_handle_server_message(data)
	
	elif state == WebSocketPeer.STATE_CLOSED:
		var reason = _client.get_close_reason()
		status_text.text = "Отключено: " + (reason if reason else "Неизвестная причина")
		status_label.text = "Отключено"
		authenticated = false
		waiting_for_response = false
		_update_auth_buttons()
		
		if call_status != "idle":
			_end_call("Соединение разорвано")

func _on_user_selected(index: int):
	selected_user_id = user_list.get_item_metadata(index)
	chat_display.clear()
	_send_to_server({
		"type": "get_messages",
		"other_user_id": selected_user_id
	})

func _on_register_button_pressed():
	if !auth_buttons_enabled:
		return
	
	set_auth_buttons_enabled(false)
	
	username = username_input.text.strip_edges()
	password = password_input.text.strip_edges()
	
	if username.length() < 3 or password.length() < 6:
		status_text.text = "Имя (3+ символов) и пароль (6+ символов)"
		set_auth_buttons_enabled(true)
		return
	
	var body = JSON.stringify({
		"username": username,
		"password": password
	})
	
	var headers = ["Content-Type: application/json"]
	var error = http_request.request(http_url + "/register", headers, HTTPClient.METHOD_POST, body)
	
	if error != OK:
		status_text.text = "Ошибка при отправке запроса"
		set_auth_buttons_enabled(true)
		return
	
	status_text.text = "Регистрация..."

func _on_login_button_pressed():
	if !auth_buttons_enabled:
		return
	
	set_auth_buttons_enabled(false)
	
	username = username_input.text.strip_edges()
	password = password_input.text.strip_edges()
	
	if username.length() < 3 or password.length() < 6:
		status_text.text = "Имя (3+ символов) и пароль (6+ символов)"
		set_auth_buttons_enabled(true)
		return
	
	if _client.get_ready_state() != WebSocketPeer.STATE_CLOSED:
		_client.close()
	
	_client = WebSocketPeer.new()
	var err = _client.connect_to_url(server_url)
	
	if err != OK:
		status_text.text = "Ошибка подключения"
		set_auth_buttons_enabled(true)
		return
	
	status_text.text = "Подключение..."

func _update_auth_buttons():
	login_button.disabled = waiting_for_response
	register_button.disabled = waiting_for_response

func force_ui_update():
	pass

func _on_message_submitted(text: String):
	if text.strip_edges() == "":
		return
	
	if selected_user_id == "":
		status_label.text = "Выберите пользователя"
		return
	
	var receiver_data = null
	for user in all_users:
		if user["id"] == selected_user_id:
			receiver_data = {
				"id": user["id"],
				"username": user["username"],
				"is_online": user["is_online"]
			}
			break
	
	if receiver_data and not _is_user_in_chat_partners(selected_user_id):
		chat_partners.append(receiver_data)
		_update_user_list(chat_partners)
	
	_display_message({
		"sender": user_id,
		"sender_name": username,
		"message": text,
		"time": Time.get_time_string_from_system(),
		"is_self": true
	})
	
	_send_to_server({
		"type": "private_message",
		"receiver_id": selected_user_id,
		"message": text
	})
	
	message_input.clear()

func _on_call_button_pressed():
	if selected_user_id == "":
		status_label.text = "Выберите пользователя"
		return
	
	if not _setup_webrtc_connection():
		_end_call("Ошибка инициализации соединения")
		return
	
	var channel_config = {
		"negotiated": true,
		"id": 1
	}
	data_channel = peer_connection.create_data_channel("data", channel_config)
	if data_channel:
		data_channel.message_received.connect(_on_data_channel_message)
		print("Data channel успешно создан")
	else:
		push_error("Ошибка создания Data Channel")
		_end_call("Ошибка создания канала данных")
		return
	
	if peer_connection.create_offer() != OK:
		push_error("Ошибка создания офера")
		_end_call("Ошибка создания запроса звонка")
		return
	
	call_status = "calling"
	current_call = {
		"receiver_id": selected_user_id,
		"status": "outgoing"
	}
	call_status_label.text = "Звонок..."
	main_panel.hide()
	call_panel.show()

func _on_hangup_button_pressed():
	_end_call("Звонок завершен")

func _on_accept_call_pressed():
	if not current_call.has("caller_id"):
		return
	
	peer_connection = WebRTCPeerConnection.new()
	
	peer_connection.session_description_created.connect(_on_session_created)
	peer_connection.ice_candidate_created.connect(_on_ice_candidate_created)
	peer_connection.data_channel_received.connect(_on_data_channel_received)
	
	var config = {
		"iceServers": ice_servers
	}
	if peer_connection.initialize(config) != OK:
		push_error("Ошибка инициализации WebRTC соединения")
		return
	
	if peer_connection.set_remote_description("offer", current_call["caller_sdp"]) != OK:
		push_error("Ошибка установки удаленного описания")
		return
	
	if peer_connection.create_answer() != OK:
		push_error("Ошибка создания ответа")
		return
	
	call_status = "in_call"
	call_status_label.text = "Идет звонок..."
	incoming_call_panel.hide()
	call_panel.show()

func _on_reject_call_pressed():
	_send_to_server({
		"type": "call_end",
		"caller_id": current_call["caller_id"]
	})
	_end_call("Звонок отклонен")
	incoming_call_panel.hide()

func _on_session_created(type: String, sdp: String):
	if peer_connection.set_local_description(type, sdp) != OK:
		push_error("Ошибка установки локального описания")
		_end_call("Ошибка установки соединения")
		return
	
	if type == "offer":
		_send_offer(sdp)
	elif type == "answer":
		_send_answer(sdp)

func _on_ice_candidate(candidate_data):
	_send_ice_candidate(candidate_data)

func _send_to_server(data: Dictionary):
	if _client.get_ready_state() == WebSocketPeer.STATE_OPEN:
		var json = JSON.stringify(data)
		print("Отправка на сервер: ", json)
		_client.send_text(json)
	else:
		push_error("WebSocket не подключен")

func _handle_server_message(data):
	print("Получено сообщение: ", data)
	
	if typeof(data) != TYPE_DICTIONARY:
		push_error("Некорректный формат данных от сервера")
		return
	
	if not data.has("type"):
		push_error("Сообщение от сервера не содержит тип")
		return
	
	match data.get("type"):
		"auth_success":
			waiting_for_response = false
			_update_auth_buttons()
			user_id = data["user_id"]
			username = data["username"]
			status_text.text = "Успешный вход!"
			status_label.text = "Подключен как: " + username
			auth_panel.hide()
			main_panel.show()
			
			await get_tree().create_timer(0.1).timeout
			_send_to_server({"type": "get_users"})
		
		"users_list":
			all_users = data["users"]
			if data.has("chat_partners"):
				chat_partners = []
				for partner in data["chat_partners"]:
					for user in all_users:
						if user["id"] == partner["id"]:
							chat_partners.append({
								"id": user["id"],
								"username": user["username"],
								"is_online": user["is_online"]
							})
							break
			_update_user_list(chat_partners)
		
		"user_status":
			var user_id_to_update = data.get("user_id")
			if user_id_to_update:
				for i in range(user_list.get_item_count()):
					if user_list.get_item_metadata(i) == user_id_to_update:
						var username = data.get("username", "unknown")
						var is_online = data.get("is_online", false)
						var status_text = " (online)" if is_online else " (offline)"
						user_list.set_item_text(i, username + status_text)
						user_list.set_item_custom_fg_color(i, Color.GREEN if is_online else Color.LIGHT_GRAY)
						break
		
		"private_message":
			if selected_user_id == data["sender"]:
				_display_message({
					"sender": data["sender"],
					"sender_name": data["sender_name"],
					"message": data["message"],
					"time": data["time"],
					"is_self": false
				})
			else:
				pass
		
		"message_history":
			if data.has("messages"):
				chat_display.clear()
				for msg in data["messages"]:
					_display_message({
						"sender": msg["sender_id"],
						"sender_name": msg["sender_name"],
						"message": msg["message"],
						"time": msg["timestamp"],
						"is_self": msg["sender_id"] == user_id
					})
		
		"call_offer":
			current_call = {
				"caller_id": data.get("caller_id"),
				"caller_name": data.get("caller_name", "unknown"),
				"caller_sdp": data.get("sdp_offer"),
				"status": "incoming"
			}
			call_status = "incoming_call"
			caller_label.text = "Входящий вызов от " + current_call["caller_name"]
			main_panel.hide()
			incoming_call_panel.show()
		
		"call_answer":
			if peer_connection and current_call.get("status") == "calling":
				if peer_connection.set_remote_description("answer", data.get("sdp_answer")) != OK:
					status_label.text = "Ошибка установки удаленного описания"
					return
				call_status = "in_call"
				call_status_label.text = "Идет звонок с " + current_call.get("receiver_name", "")
		
		"call_ice_candidate":
			if peer_connection:
				var media = data.get("media", "")
				var index = data.get("index", 0)
				var name_str = data.get("name", "")
				if peer_connection.add_ice_candidate(media, index, name_str) != OK:
					print("Предупреждение: не удалось добавить ICE кандидат (возможно, remote description еще не установлен)")
		
		"call_end":
			_end_call(data.get("reason", "Звонок завершен"))
			
		"file_message":
			_display_message({
				"sender": data["sender"],
				"sender_name": data["sender_name"],
				"message": "[Файл: %s]" % data["file_name"],
				"time": data["time"],
				"is_self": data["sender"] == user_id,
				"file_url": data["file_url"],
				"file_name": data["file_name"]
			})
		_:
			push_error("Неизвестный тип сообщения: " + str(data.get("type")))

func _display_message(msg: Dictionary):
	var formatted_msg = ""
	var time = msg.get("time", "").substr(0, 5)
	var sender_name = msg.get("sender_name", "Неизвестно")
	var message = msg.get("message", "")
	var is_self = msg.get("is_self", false)
	var current_sender = msg["sender"]
	
	var show_header = true
	if current_sender == last_sender and is_self == last_is_self:
		show_header = false
	
	if show_header:
		if is_self:
			formatted_msg += "[left][color=#ff8906]Вы[/color]"
		else:
			formatted_msg += "[right][color=#f25f4c]" + sender_name + "[/color]"
		formatted_msg += " [color=#a7a9be]" + time + "[/color]"
		if is_self:
			formatted_msg += "[/left]"
		else:
			formatted_msg += "[/right]"
		formatted_msg += "\n"
	
	if msg.has("file_url"):
		var file_link = "[url=%s]Скачать %s[/url]" % [msg["file_url"], msg["file_name"]]
		if is_self:
			formatted_msg += "[left][color=#fffffe][bgcolor=#242629] " + file_link + " [/bgcolor][/color][/left]"
		else:
			formatted_msg += "[right][color=#fffffe][bgcolor=#242629] " + file_link + " [/bgcolor][/color][/right]"
	else:
		if is_self:
			formatted_msg += "[left][color=#fffffe][bgcolor=#242629] " + message + " [/bgcolor][/color][/left]"
		else:
			formatted_msg += "[right][color=#fffffe][bgcolor=#242629] " + message + " [/bgcolor][/color][/right]"
	
	formatted_msg += "\n\n"
	
	chat_display.append_text(formatted_msg)
	
	last_sender = current_sender
	last_is_self = is_self
	
	await get_tree().process_frame
	chat_display.scroll_to_line(chat_display.get_line_count())

func _update_user_list(users: Array):
	user_list.clear()
	
	if users.size() == 0:
		var item = user_list.add_item("Нет контактов")
		user_list.set_item_custom_fg_color(item, COLOR_TEXT_SECONDARY)
		return
	
	users.sort_custom(_sort_users)
	
	for user in users:
		var idx = user_list.add_item(user["username"])
		user_list.set_item_metadata(idx, user["id"])
		
		var status_color = COLOR_ONLINE if user["is_online"] else COLOR_OFFLINE
		user_list.set_item_custom_fg_color(idx, status_color)
		
		var status_icon = "● " if user["is_online"] else "○ "
		user_list.set_item_text(idx, status_icon + user_list.get_item_text(idx))

func _end_call(reason: String):
	if peer_connection != null:
		peer_connection.close()
		peer_connection = null
	
	if data_channel != null:
		data_channel.close()
		data_channel = null
	
	if call_status == "in_call":
		var caller_id = current_call["caller_id"] if current_call.has("caller_id") else user_id
		_send_to_server({
			"type": "call_end",
			"caller_id": caller_id,
			"duration": 0
		})
	
	call_status = "idle"
	current_call = {}
	call_panel.hide()
	incoming_call_panel.hide()
	status_label.text = reason
	main_panel.show()

func _on_http_request_request_completed(result, response_code, headers, body):
	set_auth_buttons_enabled(true)
	waiting_for_response = false
	_update_auth_buttons()
	
	if result != HTTPRequest.RESULT_SUCCESS:
		status_text.text = "HTTP запрос не удался"
		return
	
	var response = JSON.parse_string(body.get_string_from_utf8())
	if response_code == 200:
		status_text.text = "Регистрация успешна! Входим..."
		username = username_input.text
		password = password_input.text
		_on_login_button_pressed()
	else:
		status_text.text = "Ошибка: " + response.get("detail", "Неизвестная ошибка")
		
	if body.get_string_from_utf8().begins_with("{"):
		if response_code == 200 and response.has("filename"):
			_send_to_server({
				"type": "file_message",
				"receiver_id": selected_user_id,
				"file_name": response["original_name"],
				"file_url": http_url + "/download/" + response["filename"],
				"file_size": FileAccess.open(current_upload_file, FileAccess.READ).get_length()
			})
			
			_display_message({
				"sender": user_id,
				"sender_name": username,
				"message": "[Файл: %s]" % response["original_name"],
				"time": Time.get_time_string_from_system(),
				"is_self": true,
				"file_url": response["filename"],
				"file_name": response["original_name"]
			})
			
			current_upload_file = ""
			current_upload_name = ""

func _exit_tree():
	if _client != null:
		_client.close()
	if peer_connection != null:
		peer_connection.close()

func _on_peer_connected(id: int):
	var peer = multiplayer_peer.get_peer(id)
	peer.session_description_created.connect(_on_session_created.bind(id))
	peer.ice_candidate_created.connect(_on_ice_candidate.bind(id))
	
	if call_status == "calling":
		peer.create_offer()

func _on_peer_disconnected(id: int):
	_end_call("Соединение разорвано")

func _setup_connection():
	peer_id = multiplayer_peer.get_unique_id()
	
	multiplayer_peer.peer_connected.connect(_on_peer_connected)
	multiplayer_peer.peer_disconnected.connect(_on_peer_disconnected)
	
	_configure_channels()

func _send_offer(sdp: String):
	if current_call.has("receiver_id"):
		_send_to_server({
			"type": "call_offer",
			"receiver_id": current_call["receiver_id"],
			"sdp_offer": sdp,
			"caller_name": username
		})

func _send_answer(sdp: String):
	if current_call.has("caller_id"):
		_send_to_server({
			"type": "call_answer",
			"caller_id": current_call["caller_id"],
			"sdp_answer": sdp
		})

func _send_ice_candidate(candidate_data: Dictionary):
	if current_call.has("caller_id") || current_call.has("receiver_id"):
		var target_id = current_call.get("caller_id", current_call.get("receiver_id", ""))
		if target_id != "":
			_send_to_server({
				"type": "call_ice_candidate",
				"target_id": target_id,
				"candidate": candidate_data
			})

func _on_search_text_changed(new_text: String):
	if new_text.strip_edges() == "":
		_update_user_list(chat_partners)
		return
	
	filtered_users = []
	for user in all_users:
		if new_text.to_lower() in user["username"].to_lower() and user["id"] != user_id:
			filtered_users.append(user)
	
	_update_user_list(filtered_users)

func _on_search_submitted(text: String):
	if filtered_users.size() > 0:
		user_list.select(0)
		_on_user_selected(0)

func _is_user_in_chat_partners(user_id: String) -> bool:
	for user in chat_partners:
		if user["id"] == user_id:
			return true
	return false

func _configure_channels():
	var channel_config = {
		"transfer_mode": MultiplayerPeer.TRANSFER_MODE_RELIABLE,
		"id": 1,
		"ordered": true
	}
	
	if multiplayer_peer.create_channel(channel_config) != OK:
		push_error("Ошибка создания канала")
		return

func _on_ice_candidate_created(mid: String, index: int, sdp: String):
	print("Получен ICE-кандидат: ", mid, " ", index, " ", sdp)
	
	var candidate_data = {
		"sdpMid": mid,
		"sdpMLineIndex": index,
		"candidate": sdp
	}
	
	_send_to_server({
		"type": "ice_candidate",
		"candidate": candidate_data,
		"target_id": current_call.get("receiver_id", current_call.get("caller_id", ""))
	})

func _on_data_channel_message(message: String):
	print("Получено сообщение через data channel: ", message)

func _on_data_channel_received(channel: WebRTCDataChannel):
	data_channel = channel
	data_channel.message_received.connect(_on_data_channel_message)
	print("Data channel получен")

func set_auth_buttons_enabled(enabled: bool):
	auth_buttons_enabled = enabled
	
	login_button.modulate = Color(1, 1, 1, 0.5) if !enabled else Color.WHITE
	register_button.modulate = Color(1, 1, 1, 0.5) if !enabled else Color.WHITE
	
	login_button.queue_redraw()
	register_button.queue_redraw()
	status_text.queue_redraw()

func _setup_ui():
	var theme = Theme.new()
	self.theme = theme
	
	var bg_style = StyleBoxFlat.new()
	bg_style.bg_color = COLOR_BG
	self.add_theme_stylebox_override("panel", bg_style)
	
	var card_style = StyleBoxFlat.new()
	card_style.bg_color = COLOR_CARD
	card_style.corner_radius_top_left = 8
	card_style.corner_radius_top_right = 8
	card_style.corner_radius_bottom_right = 8
	card_style.corner_radius_bottom_left = 8
	card_style.border_width_left = 1
	card_style.border_width_right = 1
	card_style.border_width_top = 1
	card_style.border_width_bottom = 1
	card_style.border_color = Color(COLOR_CARD).lightened(0.1)
	
	for panel in [auth_panel, main_panel, call_panel, incoming_call_panel]:
		if panel:
			panel.add_theme_stylebox_override("panel", card_style)
	
	var button_style = StyleBoxFlat.new()
	button_style.bg_color = COLOR_PRIMARY
	button_style.corner_radius_top_left = 6
	button_style.corner_radius_top_right = 6
	button_style.corner_radius_bottom_right = 6
	button_style.corner_radius_bottom_left = 6
	button_style.content_margin_left = 8
	button_style.content_margin_right = 8
	button_style.content_margin_top = 8
	button_style.content_margin_bottom = 8
	
	var button_hover = button_style.duplicate()
	button_hover.bg_color = Color(COLOR_PRIMARY).lightened(0.1)
	
	var button_pressed = button_style.duplicate()
	button_pressed.bg_color = Color(COLOR_PRIMARY).darkened(0.1)
	
	_apply_button_style(login_button, button_style, button_hover, button_pressed)
	_apply_button_style(register_button, button_style, button_hover, button_pressed)
	_apply_button_style(call_button, button_style, button_hover, button_pressed)
	_apply_button_style(accept_call_button, button_style, button_hover, button_pressed)
	
	var reject_style = button_style.duplicate()
	reject_style.bg_color = COLOR_WARNING
	var reject_hover = reject_style.duplicate()
	reject_hover.bg_color = Color(COLOR_WARNING).lightened(0.1)
	var reject_pressed = reject_style.duplicate()
	reject_pressed.bg_color = Color(COLOR_WARNING).darkened(0.1)
	_apply_button_style(reject_call_button, reject_style, reject_hover, reject_pressed)
	
	var input_style = StyleBoxFlat.new()
	input_style.bg_color = COLOR_INPUT
	input_style.set_corner_radius_all(6)
	input_style.border_width_left = 1
	input_style.border_width_right = 1
	input_style.border_width_top = 1
	input_style.border_width_bottom = 1
	input_style.border_color = Color(COLOR_INPUT).lightened(0.2)
	
	for input in [username_input, password_input, message_input, search]:
		if input:
			input.add_theme_stylebox_override("normal", input_style)
			input.add_theme_color_override("font_color", COLOR_TEXT)
			input.add_theme_font_size_override("font_size", 16)
	
	var list_style_normal = StyleBoxFlat.new()
	var list_style_hover = StyleBoxFlat.new()
	list_style_hover.bg_color = COLOR_HOVER
	var list_style_selected = StyleBoxFlat.new()
	list_style_selected.bg_color = COLOR_PRIMARY
	
	if user_list:
		user_list.add_theme_stylebox_override("panel", StyleBoxEmpty.new())
		user_list.add_theme_stylebox_override("normal", list_style_normal)
		user_list.add_theme_stylebox_override("hover", list_style_hover)
		user_list.add_theme_stylebox_override("selected", list_style_selected)
	
	var chat_style = StyleBoxFlat.new()
	chat_style.bg_color = COLOR_BG
	
	if chat_display:
		chat_display.add_theme_stylebox_override("normal", StyleBoxEmpty.new())
		chat_display.add_theme_font_size_override("font_size", 16)
		chat_display.add_theme_color_override("default_color", COLOR_TEXT)
		chat_display.bbcode_enabled = true
		chat_display.scroll_following = true
		chat_display.selection_enabled = false
	
	for label in [status_text, status_label, call_status_label, caller_label]:
		if label:
			label.add_theme_font_size_override("font_size", 16)
			label.add_theme_color_override("font_color", COLOR_TEXT)

func _apply_button_style(button: Button, normal: StyleBox, hover: StyleBox, pressed: StyleBox):
	if button:
		button.add_theme_stylebox_override("normal", normal)
		button.add_theme_stylebox_override("hover", hover)
		button.add_theme_stylebox_override("pressed", pressed)
		button.add_theme_color_override("font_color", COLOR_TEXT)
		button.add_theme_font_size_override("font_size", 16)

func _on_button_hover(button: Button):
	var tween = create_tween()
	tween.tween_property(button, "scale", BUTTON_SCALE, ANIM_SPEED).set_trans(Tween.TRANS_BACK)

func _on_button_unhover(button: Button):
	var tween = create_tween()
	tween.tween_property(button, "scale", Vector2.ONE, ANIM_SPEED).set_trans(Tween.TRANS_BACK)

func _sort_users(a: Dictionary, b: Dictionary) -> bool:
	if a["is_online"] != b["is_online"]:
		return a["is_online"] > b["is_online"]
	return a["username"] < b["username"]

func _on_file_button_pressed():
	file_dialog.popup_centered(Vector2(800, 600))

func _on_file_selected(path: String):
	var file = FileAccess.open(path, FileAccess.READ)
	if not file:
		status_label.text = "Ошибка открытия файла"
		return
	
	if file.get_length() > MAX_FILE_SIZE:
		status_label.text = "Файл слишком большой (макс. 5МБ)"
		return
	
	var ext = path.get_extension().to_lower()
	if not ALLOWED_EXTENSIONS.has(ext):
		status_label.text = "Неподдерживаемый тип файла"
		return
	
	current_upload_file = path
	current_upload_name = path.get_file()
	
	if ext in ["png", "jpg", "jpeg", "gif"]:
		_show_image_preview(path)
	elif ext == "mp4":
		_show_video_preview(path)
	else:
		_confirm_file_send()

func _show_image_preview(path: String):
	pass

func _show_video_preview(path: String):
	pass

func _confirm_file_send():
	var confirm_dialog = AcceptDialog.new()
	confirm_dialog.title = "Отправить файл?"
	confirm_dialog.dialog_text = "Отправить %s (%s)?" % [current_upload_name, _format_file_size(current_upload_file)]
	confirm_dialog.confirmed.connect(_upload_file)
	add_child(confirm_dialog)
	confirm_dialog.popup_centered()

func _format_file_size(path: String) -> String:
	var size = FileAccess.get_file_as_bytes(path).size()
	if size < 1024:
		return "%d B" % size
	elif size < 1024 * 1024:
		return "%.1f KB" % (size / 1024.0)
	else:
		return "%.1f MB" % (size / (1024.0 * 1024.0))

func _upload_file():
	if current_upload_file == "":
		return
	
	var file = FileAccess.open(current_upload_file, FileAccess.READ)
	var file_data = file.get_buffer(file.get_length())
	
	var headers = ["Content-Type: application/octet-stream", "X-Filename: " + current_upload_name]
	var error = http_request.request(http_url + "/upload", headers, HTTPClient.METHOD_POST, file_data)
	
	if error != OK:
		status_label.text = "Ошибка при отправке файла"
		return
	
	status_label.text = "Отправка файла..."

func _on_meta_clicked(meta):
	if meta is String and meta.begins_with("http"):
		OS.shell_open(meta)

func _setup_webrtc_connection() -> bool:
	peer_connection = WebRTCPeerConnection.new()
	
	if peer_connection == null:
		push_error("Не удалось создать WebRTCPeerConnection")
		return false
	
	var connect_result = true
	connect_result = connect_result and (peer_connection.session_description_created.connect(_on_session_created) == OK)
	connect_result = connect_result and (peer_connection.ice_candidate_created.connect(_on_ice_candidate_created) == OK)
	
	if not connect_result:
		push_error("Ошибка подключения сигналов WebRTC")
		return false
	
	var config = {
		"iceServers": ice_servers
	}
	
	if peer_connection.initialize(config) != OK:
		push_error("Ошибка инициализации WebRTC соединения")
		return false
	
	return true

func _on_ice_connection_state_changed(new_state: int):
	var states = {
		0: "NEW",
		1: "CHECKING", 
		2: "CONNECTED",
		3: "COMPLETED",
		4: "FAILED",
		5: "DISCONNECTED",
		6: "CLOSED"
	}
	var state_name = states.get(new_state, "UNKNOWN")
	print("ICE соединение изменило состояние: ", state_name)
	
	if new_state == 4:
		_end_call("Соединение не установлено")

func _on_connection_state_changed(new_state: int):
	var states = {
		0: "NEW",
		1: "CONNECTING",
		2: "CONNECTED", 
		3: "DISCONNECTED",
		4: "FAILED",
		5: "CLOSED"
	}
	var state_name = states.get(new_state, "UNKNOWN")
	print("Состояние соединения изменилось: ", state_name)
	
	if new_state == 2:
		call_status_label.text = "Соединение установлено"
	elif new_state == 4:
		_end_call("Ошибка соединения")