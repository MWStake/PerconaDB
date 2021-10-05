<?php

/**
 * Copyright (C) 2019  NicheWork, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @author Mark A. Hershberger <mah@nichework.com>
 */
namespace MediaWiki\Extension\PerconaDB;

use DatabaseUpdater;
use IDatabase;

class Hook {
	/**
	 * Fired when MediaWiki is updated to allow extensions to update the
	 * database
	 *
	 * @param DatabaseUpdater $upd
	 * @see https://www.mediawiki.org/wiki/Manual:Hooks/LoadExtensionSchemaUpdates
	 */
	public static function onLoadExtensionSchemaUpdates(
		DatabaseUpdater $upd
	) {
		$upd->addExtensionUpdate( [ __CLASS__ . '::ensureNoMyISAMTables' ] );
		$upd->addExtensionUpdate( [ __CLASS__ . '::ensurePrimaryKeys' ] );
	}

	/**
	 * Ensure that all tables have primary keys.
	 *
	 * @param DatabaseUpdater $upd
	 */
	public static function ensurePrimaryKeys(
		DatabaseUpdater $upd
	) {
		$pkCreator = new PrimaryKeyCreator( $upd );
		$pkCreator->execute();
	}

	/**
	 * Ensure that there are no tables using MyISAM.
	 *
	 * @param DatabaseUpdater $upd
	 */
	public static function ensureNoMyISAMTables( DatabaseUpdater $upd ) {
		$dbw = $upd->getDB();
		$tables = self::getMyISAMTables( $dbw );
		if ( count( $tables ) ) {
			$upd->output( "Converting tables to InnoDB...\n" );
			foreach ( $tables as $table ) {
				$upd->output( "...$table\n" );
				self::convertToInnoDB( $dbw, $table );
			}
		}
	}

	/**
	 * Return a list of tables that use the MyISAM engine.
	 *
	 * @param $dbw
	 * @return array[]
	 */
	public static function getMyISAMTables( IDatabase $dbw ) {
		$dbName = $dbw->addQuotes( $dbw->getDBName() );

		$ret = [];
		$res = $dbw->query(
			"SELECT table_name
               FROM information_schema.tables
              WHERE engine='MyISAM'
                AND table_schema=$dbName"
		);
		if ( $res->numRows() ) {
			foreach ( $res as $row ) {
				$ret[] = $dbw->addIdentifierQuotes( $row->table_name );
			}
		}
		return $ret;
	}

	/**
	 * Convert a table to use the InnoDB engine.
	 */
	public static function convertToInnoDB( IDatabase $dbw, string $table ) {
		return $dbw->query( "ALTER TABLE {$table} ENGINE InnoDB" );
	}

    public static function onSMWBeforeCreateTablesComplete(
		array $tables, $messageReporter
	) {
		$primaryKeys = PrimaryKeyCreator::getSMWIndexMap();

		/**
		 * @var \Onoi\MessageReporter\MessageReporter
		 */
		$messageReporter->reportMessage( "Setting primary indices.\n" );
		/**
		 * @var \SMW\SQLStore\TableBuilder\Table[]
		 */
		foreach ( $tables as $table ) {
			$key = $primaryKeys[$table->getName()] ?? false;
			if ( is_string( $key ) ) {
				$table->setPrimaryKey( $primaryKeys[$table->getName()] );
			} elseif ( is_array( $key ) && is_callable( [ $table, $key[0] ] ) ) {
				$method = array_shift( $key );
				$table->$method( ...$key );
			}
		}
		$messageReporter->reportMessage( "\ndone.\n" );
	}
}
