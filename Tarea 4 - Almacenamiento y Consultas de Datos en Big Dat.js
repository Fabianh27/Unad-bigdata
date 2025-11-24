
//  Tarea 4 - Almacenamiento y Consultas de Datos en Big Data
//  UNAD - Big Data
//  nombre: Héctor Fabián Caballero Villalba
//  UNIVERSIDAD abierta uy a distancia Unad

// 1. INSERTAR DOCUMENTO 

db.ESTUDIANTES.insertOne({
    "Car ID": 3005,
    "Brand": "Mazda",
    "Model": "Mazda 2",
    "Year": 2016,
    "Engine Size": 1.5,
    "Fuel Type": "Petrol",
    "Transmission": "Automatic",
    "Mileage": 78000,
    "State": "Used",
    "Price": 10500
});

// 2. CONSULTA BÁSICA // Buscar un Mazda 2

db.ESTUDIANTES.find({ "Model": "Mazda 2" });

// 3. CONSULTA CON OPERADORES ($lt, $gt )

// Buscar vehículos con precio menor a 10,000
db.ESTUDIANTES.find({
    "Price": { "$lt": 10000 }
});

// 4. ELIMINAR DOCUMENTO // Eliminar un Mazda 2 insertado previamente

db.ESTUDIANTES.deleteOne({
    "Model": "Mazda 2"
});

// 5. CONSULTA PARA LISTAR TODOS LOS DOCUMENTOS

db.ESTUDIANTES.find();

// 6. CONSULTA CON PROYECCIÓN (mostrar solo algunas columnas)

db.ESTUDIANTES.find(
    { "Brand": "Ford" },
    { "_id": 0, "Brand": 1, "Model": 1, "Year": 1 }
);

// 7. CONSULTA DE AGREGACIÓN //  Promedio, suma y conteo

db.getCollection('ESTUDIANTES').aggregate([
    {
        $group: {
            _id: "$Brand",
            avgMileage: { $avg: "$Mileage" }
        }
    },
    {
        $group: {
            _id: "$Brand",
            totalMileage: { $sum: "$Mileage" }
        }
    },
    {
        $count: "total_cars"
    }
]);
