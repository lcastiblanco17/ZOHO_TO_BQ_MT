import logging

def setup_logger(name: str = __name__) -> logging.Logger:
    """
    Configura y devuelve un logger con formato estándar.
    Solo debe llamarse una vez por módulo.
    """
    logger = logging.getLogger(name)
    #verifico que si ya tengo un handler para evitar mensajes duplicados
    if not logger.handlers: 
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        #Creo el handler y le digo que quiero los mensajes por consola, le paso la configuracion y se lo paso al logger
        handler = logging.StreamHandler()  
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
