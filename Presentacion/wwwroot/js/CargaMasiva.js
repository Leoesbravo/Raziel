function validateFile() {
    var allowedExtension = ['xls', 'xlsx', 'csv'];
    var fileExtension = document.getElementById('inpxls').value.split('.').pop().toLowerCase();
    var isValidFile = false;

    for (var index in allowedExtension) {

        if (fileExtension === allowedExtension[index]) {
            isValidFile = true;
            break;
        }
    }

    if (!isValidFile) {
        alert('Las extensiones permitidas son : *.' + allowedExtension.join(', *.'));
        document.getElementById('inpxls').value = "";
    }

    return isValidFile;
}