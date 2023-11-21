from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, ForeignKey
from sqlalchemy.orm import relationship

Base = declarative_base()


class Proteina(Base):
    __tablename__ = 'proteinas'
    id = Column(Integer, primary_key=True)
    entry_name = Column(String, unique=True, nullable=False)
    data_class = Column(String)
    molecule_type = Column(String)
    sequence_length = Column(Integer)
    accessions = relationship("Accession",
                              back_populates="proteina")  # Podría necesitar una tabla separada si hay múltiples accessions
    created_date = Column(Date)
    sequence_update_date = Column(Date)
    annotation_update_date = Column(Date)
    description = Column(String)
    gene_name = Column(String)  # Podría necesitar una tabla separada si hay múltiples nombres de genes
    organism = Column(String)
    organelle = Column(String)
    organism_classification = Column(String)  # Podría necesitar una tabla separada para una clasificación jerárquica
    taxonomy_id = Column(String)  # Podría necesitar una tabla separada si hay múltiples ID de taxonomía
    host_organism = Column(String)  # Similar a gene_name y taxonomy_id
    host_taxonomy_id = Column(String)  # Similar a gene_name y taxonomy_id
    comments = Column(String)  # Podría necesitar una tabla separada si hay múltiples comentarios

    pdb_references = relationship("PDBReference", back_populates="proteina")
    go_terms = relationship("GOTerm", back_populates="proteina")

    keywords = Column(String)  # Similar a comments
    protein_existence = Column(Integer)
    seqinfo = Column(String)


class Accession(Base):
    __tablename__ = 'accessions'
    id = Column(Integer, primary_key=True)
    accession_code = Column(String, unique=True, nullable=False)
    proteina_entry_name = Column(String, ForeignKey('proteinas.entry_name'))
    proteina = relationship("Proteina", back_populates="accessions")


class PDBReference(Base):
    __tablename__ = 'pdb_references'
    id = Column(Integer, primary_key=True)
    pdb_id = Column(String, nullable=False)
    proteina_id = Column(Integer, ForeignKey('proteinas.id'))
    proteina = relationship("Proteina", back_populates="pdb_references")

    method = Column(String)  # Método utilizado para la determinación de la estructura
    resolution = Column(String)  # Resolución de la estructura
    chains = Column(String)  # Cadenas involucradas


class GOTerm(Base):
    __tablename__ = 'go_terms'
    id = Column(Integer, primary_key=True)
    go_id = Column(String, nullable=False)
    proteina_id = Column(Integer, ForeignKey('proteinas.id'))
    proteina = relationship("Proteina", back_populates="go_terms")
    category = Column(String)  # Puede ser 'F', 'P' o 'C' para Función, Proceso, Componente
    description = Column(String)


Proteina.accessions = relationship("Accession", order_by=Accession.id, back_populates="proteina")
